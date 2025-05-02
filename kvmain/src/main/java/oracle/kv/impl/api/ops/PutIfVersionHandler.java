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
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;

/**
 * Server handler for {@link PutIfVersion}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | Y |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | N |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | Y |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * | PutIfVersion  | VERSION|---+----------------------+-----------------------|
 * |               |        | N |        MIN_READ      |           0           |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | Y |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | N |    old record size   |           0           |
 * +---------------------------------------------------------------------------+
 *      # = Target record matches (Y) or not (N)
 */
class PutIfVersionHandler extends BasicPutHandler<PutIfVersion> {

    PutIfVersionHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_IF_VERSION, PutIfVersion.class);
    }

    @Override
    Result execute(PutIfVersion op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        ResultValueVersion prevVal = null;
        long expTime = 0L;
        long modificationTime = 0L;
        int storageSize = -1;
        Version version = null;
        boolean wasUpdate;

        byte[] keyBytes = op.getKeyBytes();
        byte[] valueBytes = op.getValueBytes();
        Version matchVersion = op.getMatchVersion();

        assert (keyBytes != null) && (valueBytes != null) &&
            (matchVersion != null);

        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);

        OperationResult opres;
        WriteOptions options = makeOption(op.getTTL(), op.getUpdateTTL());

        final Database db = getRepNode().getPartitionDB(partitionId);

        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);

        try {
            final Choice choice = op.getReturnValueVersionChoice();
            /*
             * Ignore an existing tombstone since putIfVersion is only invoked
             * locally and should always win
             */
            final ReadOptions readOptions =
                InternalOperationHandler.RMW_EXCLUDE_TOMBSTONES;

            opres = cursor.get(keyEntry, NO_DATA, Get.SEARCH,
                               readOptions);

            if (opres == null) {
                op.addReadBytes(MIN_READ);
                wasUpdate = false;

            } else if (versionMatches(cursor, matchVersion)) {
                TableImpl table = operationHandler.
                    getAndCheckTable(op.getTableId());
                if (table != null && getHasMRCounters(table)) {
                    /* For table rows with counter CRDTs, need the
                     * CRDT values from the prev row. */
                    final DatabaseEntry prevData = new DatabaseEntry();
                    opres = cursor.get(keyEntry, prevData, Get.CURRENT,
                                       LockMode.RMW.toReadOptions());

                    op.addReadBytes(getStorageSize(cursor));
                    dataEntry = PutHandler.copyCRDTFromPrevRow(table, keyBytes,
                                                               valueBytes,
                                                               prevData,
                                                               getRepNode());

                } else {
                    op.addReadBytes(MIN_READ);
                }
                final int oldRecordSize = getStorageSize(cursor);
                op.addWriteBytes(oldRecordSize, 0, partitionId, -oldRecordSize);

                opres = putEntry(cursor, null, dataEntry, Put.CURRENT, options);

                version = getVersion(cursor);
                expTime = opres.getExpirationTime();
                modificationTime = opres.getModificationTime();
                storageSize = getStorageSize(cursor);
                wasUpdate = true;

                op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                 partitionId, storageSize);

                MigrationStreamHandle.get().addPut(keyEntry,
                                                   dataEntry,
                                                   version.getVLSN(),
                                                   modificationTime,
                                                   expTime,
                                                   false /*isTombstone*/);
            } else {
                final DatabaseEntry prevData;

                if (choice.needValue()) {
                    prevData = new DatabaseEntry();

                    opres = cursor.get(keyEntry, prevData, Get.CURRENT,
                                       LockMode.RMW.toReadOptions());

                    op.addReadBytes(getStorageSize(cursor));
                } else {
                    prevData = NO_DATA;
                    op.addReadBytes(MIN_READ);
                }

                wasUpdate = false;

                prevVal = getBeforeUpdateInfo(choice, cursor,
                                              operationHandler,
                                              prevData, opres);

                reserializeResultValue(op, prevVal);
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
