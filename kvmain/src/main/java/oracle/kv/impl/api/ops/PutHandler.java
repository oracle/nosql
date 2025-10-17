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

import java.util.List;

import oracle.kv.FaultException;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.JsonDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;

/**
 * Server handler for {@link Put}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P |           0          |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  NONE  |---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |        MIN_READ      |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * | Put           | VERSION|---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * |               +--------+---+----------------------+-----------------------|
 * |               |        | P |    old record size   |    new record size +  |
 * |               |        |   |                      |    old record size    |
 * |               |  VALUE |---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 */
public class PutHandler extends BasicPutHandler<Put> {

    public PutHandler(OperationHandler handler) {
        super(handler, OpCode.PUT, Put.class);
    }

    @Override
    public Result execute(Put op, Transaction txn, PartitionId partitionId) {

        verifyDataAccess(op);

        return put(op, txn, partitionId, getRepNode(), operationHandler,
                   opCode, false);
    }

    protected static Result.PutResult put(Put op,
                                          Transaction txn,
                                          PartitionId partitionId,
                                          RepNode repnode,
                                          OperationHandler operationHandler,
                                          OpCode opCode,
                                          boolean tombstone) {
        ResultValueVersion prevVal = null;
        long expTime = 0L;
        Version version = null;
        long creationTime = 0L;
        long modificationTime = 0L;
        int storageSize = -1;
        boolean wasUpdate;

        byte[] keyBytes = op.getKeyBytes();
        byte[] valueBytes = op.getValueBytes();

        assert (keyBytes != null) && (valueBytes != null);
        checkTombstone(tombstone, valueBytes);

        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);

        OperationResult opres = null;

        final TimeToLive ttl;
        final boolean updateTTL;
        if (tombstone) {
            ttl = repnode.getRepNodeParams().getTombstoneTTL();
            updateTTL = true;
        } else {
            ttl = op.getTTL();
            updateTTL = op.getUpdateTTL();
        }
        final WriteOptions jeOptions =
            makeOption(ttl, updateTTL, op.getTableId(), operationHandler,
                       tombstone);
        jeOptions.setAllQueryIndexes(op.getAllIndexes(), op.getAllIndexIds());
        jeOptions.setIndexesToUpdate(op.getIndexesToUpdate());

        final Database db = repnode.getPartitionDB(partitionId);

        /*
         * We first try to do a put(NO_OVERWITE). If this succeeds (i.e. the
         * row did not exist already), we are done. Otherwise, we must search
         * for the record again and then do a put(CURRENT). This is because
         * (a) the put(NO_OVERWITE) will not keep the cursor positioned on the
         * existing row and (b) we cannot just do put(OVERWRITE) because we
         * need before-update info (at the minumum the size of the record
         * before the update).
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);

        try {
            while (true) {
                if (!op.isSQLUpdate()) {
                    opres = putEntry(cursor, keyEntry, dataEntry,
                                     NO_OVERWRITE, jeOptions);
                }

                if (opres != null) {
                    final int newRecordSize = getStorageSize(cursor);
                    op.addWriteBytes(newRecordSize, getNIndexWrites(cursor),
                                     partitionId, newRecordSize);

                    version = operationHandler.getVersion(cursor);
                    expTime = opres.getExpirationTime();
                    creationTime = opres.getCreationTime();
                    modificationTime = opres.getModificationTime();
                    storageSize = getStorageSize(cursor);
                    wasUpdate = false;

                } else {
                    final Choice choice = op.getReturnValueVersionChoice();

                    long tableId = op.getTableId();
                    TableImpl table = null;
                    boolean hasCRDT = false;

                    if (!op.isSQLUpdate() && tableId > 0) {
                        table = operationHandler.
                            getAndCheckTable(op.getTableId());
                        hasCRDT = getHasMRCounters(table);
                    }

                    /* For table rows with counter CRDTs, always need the
                     * prev data. */
                    final DatabaseEntry prevData =
                        ((hasCRDT || choice.needValue()) ?
                         new DatabaseEntry() : NO_DATA);

                    opres = cursor.get(keyEntry, prevData, Get.SEARCH,
                                       LockMode.RMW.toReadOptions());

                    if (opres == null) {
                        /* Another thread deleted the record. Continue. */
                        if (op.isSQLUpdate()) {
                            throw new FaultException(
                                "Row deleted while being locked by " +
                                "UPDATE statement", true);
                        }
                        continue;
                    }

                    final boolean prevTombstone = opres.isTombstone();

                    final int oldRecordSize = getStorageSize(cursor);

                    /*
                     * Set the prev value and version. Exclude prev tombstone
                     * for table operations.
                     */
                    if (!prevTombstone || op.getTableId() == 0) {
                        if (table != null && hasCRDT && !tombstone) {
                            /*
                             * Copy over CRDT values from the previous row
                             * unless the value is a tombstone.
                             * */
                            dataEntry = copyCRDTFromPrevRow(table, keyBytes,
                                                            valueBytes,
                                                            prevData, repnode);
                        }
                        prevVal = getBeforeUpdateInfo(choice, cursor,
                                                      operationHandler,
                                                      prevData, opres);

                        /* Charge for the above search */
                        if (choice.needValue() || hasCRDT) {
                            op.addReadBytes(oldRecordSize);
                        } else if (choice.needVersion()) {
                            op.addReadBytes(MIN_READ);
                        }
                    }

                    /* Charge for the deletion of the old record */
                    op.addWriteBytes(oldRecordSize, 0,
                                     partitionId, -oldRecordSize);

                    /* Do the update */
                    opres = putEntry(cursor, null, dataEntry, CURRENT, jeOptions);

                    /* Charge for the above update */
                    final int newRecordSize = getStorageSize(cursor);
                    op.addWriteBytes(newRecordSize, getNIndexWrites(cursor),
                                     partitionId, newRecordSize);

                    expTime = opres.getExpirationTime();
                    version = operationHandler.getVersion(cursor);
                    creationTime = opres.getCreationTime();
                    modificationTime = opres.getModificationTime();
                    storageSize = getStorageSize(cursor);

                    if (!prevTombstone) {
                        wasUpdate = true;
                    } else {
                        wasUpdate = false;
                    }

                    reserializeResultValue(op, prevVal, operationHandler);
                }

                MigrationStreamHandle.get().addPut(keyEntry,
                                                   dataEntry,
                                                   version.getVLSN(),
                                                   creationTime,
                                                   modificationTime,
                                                   expTime,
                                                   tombstone);

                return new Result.PutResult(opCode,
                                            op.getReadKB(),
                                            op.getWriteKB(),
                                            prevVal,
                                            version,
                                            expTime,
                                            wasUpdate,
                                            creationTime,
                                            modificationTime,
                                            storageSize,
                                            repnode.getRepNodeId().
                                            getGroupId());
            }
        } finally {
            TxnUtil.close(cursor);
        }
    }

    protected static void checkTombstone(boolean tombstone, byte[] valueBytes) {
        if (tombstone && !Value.isTombstone(valueBytes)) {
            throw new FaultException("The value for tombstone " +
                "must be empty. length=" +
                (valueBytes.length - Value.getValueOffset(valueBytes)), true);
        }
    }

    protected static DatabaseEntry copyCRDTFromPrevRow(TableImpl table,
                                                       byte[] keyBytes,
                                                       byte[] valueBytes,
                                                       DatabaseEntry prevData,
                                                       RepNode repnode) {
        Value newRowVal = null;
        if (table.isJsonCollection()) {
            newRowVal = table.copyJsonCollectionMRCounters(valueBytes,
                                                           prevData.getData());
        } else {
            RowImpl prevRow = table.createRowFromBytes(keyBytes,
                                                       prevData.getData(),
                                                       false/*keyOnly*/,
                                                       false/*addMissingCol*/);
            RowImpl newRow = table.createRowFromBytes(keyBytes,
                                                      valueBytes,
                                                      false/*keyOnly*/,
                                                      false/*addMissingCol*/);
            RecordDefImpl rowDef = newRow.getDefinition();

            for (int i = 0; i < rowDef.getNumFields(); ++i) {

                FieldDefImpl fdef = rowDef.getFieldDef(i);
                FieldValueImpl prevFieldVal = prevRow.get(i);

                if (fdef.isMRCounter()) {
                    newRow.putInternal(i, prevFieldVal, false);

                } else if (fdef.hasJsonMRCounter()) {

                    List<TablePath> mrcounterPaths =
                        table.getSchemaMRCounterPaths(i);

                    for (TablePath path : mrcounterPaths) {
                        FieldValueImpl val =
                            prevRow.evaluateScalarPath(path, 0);
                        JsonDefImpl.insertMRCounterField(newRow, path,
                                                         val, true);
                    }
                }
            }

            newRowVal = table.
                createValueInternal((RowSerializer)newRow,
                                    (KVStoreImpl)repnode.getKVStore(), null,
                                    false);
        }

        return valueDatabaseEntry(newRowVal.toByteArray());
    }
}
