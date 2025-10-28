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

import static com.sleepycat.je.Put.CURRENT;
import static com.sleepycat.je.Put.NO_OVERWRITE;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;

/**
 * Server handler for {@link PutIfAbsent}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |           0           |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |           0           |
 * | PutIfAbsent   | VERSION|---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |           0           |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |        MIN_READ      |    new record size    |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
class PutIfAbsentHandler extends BasicPutHandler<PutIfAbsent> {

    PutIfAbsentHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_IF_ABSENT, PutIfAbsent.class);
    }

    @Override
    Result execute(PutIfAbsent op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        ResultValueVersion prevVal = null;
        long expTime = 0L;
        Version version = null;
        long creationTime = 0L;
        long modificationTime = 0L;
        int storageSize = -1;

        byte[] keyBytes = op.getKeyBytes();
        byte[] valueBytes = op.getValueBytes();

        assert (keyBytes != null) && (valueBytes != null);

        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);

        OperationResult opres;
        final WriteOptions jeOptions = makeOption(op.getTTL(),
                                                  op.getUpdateTTL(),
                                                  op.getTableId(),
                                                  getOperationHandler(),
                                                  false /* not tombstone */);

        final Database db = getRepNode().getPartitionDB(partitionId);

        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);

        try {
            while (true) {
                opres = putEntry(cursor, keyEntry, dataEntry,
                                 NO_OVERWRITE, jeOptions);

                if (opres != null) {
                    version = getVersion(cursor);
                    expTime = opres.getExpirationTime();
                    creationTime = opres.getCreationTime();
                    modificationTime = opres.getModificationTime();
                    storageSize = getStorageSize(cursor);

                    op.addReadBytes(MIN_READ);
                    op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                     partitionId, storageSize);

                    MigrationStreamHandle.get().addPut(keyEntry,
                                                       dataEntry,
                                                       version.getVLSN(),
                                                       creationTime,
                                                       modificationTime,
                                                       expTime,
                                                       false /*isTombstone*/);
                } else {
                    final Choice choice = op.getReturnValueVersionChoice();


                    final DatabaseEntry prevData =
                        (choice.needValue() ?
                         new DatabaseEntry() : NO_DATA);

                    opres = cursor.get(keyEntry, prevData, Get.SEARCH,
                                       LockMode.DEFAULT.toReadOptions());

                    if (opres == null) {
                        /* Another thread deleted the record. Continue. */
                        continue;
                    }

                    if (opres.isTombstone()) {
                        opres = putEntry(cursor, null, dataEntry,
                                         CURRENT, jeOptions);
                        storageSize = getStorageSize(cursor);
                        op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                         partitionId, storageSize);
                        version = getVersion(cursor);
                        expTime = opres.getExpirationTime();
                        creationTime = opres.getCreationTime();
                        modificationTime = opres.getModificationTime();

                        MigrationStreamHandle.get().
                            addPut(keyEntry,
                                   dataEntry,
                                   version.getVLSN(),
                                   creationTime,
                                   modificationTime,
                                   expTime,
                                   false /*isTombstone*/);
                        op.addReadBytes(MIN_READ);

                    } else {
                        if (choice == Choice.NONE) {
                            op.addReadBytes(MIN_READ);

                        } else {
                            prevVal = getBeforeUpdateInfo(choice, cursor,
                                                          operationHandler,
                                                          prevData, opres);
                            /* Charge for the above search */
                            if (choice.needValue()) {
                                op.addReadBytes(getStorageSize(cursor));
                            } else {
                                op.addReadBytes(MIN_READ);
                            }
                        }
                    }

                    reserializeResultValue(op, prevVal);

                }

                return new Result.PutResult(getOpCode(),
                                            op.getReadKB(),
                                            op.getWriteKB(),
                                            prevVal,
                                            version,
                                            expTime,
                                            false /*wasUpdate*/,
                                            creationTime,
                                            modificationTime,
                                            storageSize,
                                            getRepNode().getRepNodeId().
                                            getGroupId());
            }
        } finally {
            TxnUtil.close(cursor);
        }
    }
}
