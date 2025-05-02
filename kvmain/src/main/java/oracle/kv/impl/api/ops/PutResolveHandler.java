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

import static com.sleepycat.je.Put.NO_OVERWRITE;
import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;

import java.util.List;

import oracle.kv.FaultException;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.xregion.agent.TargetTableEvolveException;
import oracle.kv.impl.xregion.resolver.ConflictResolver;
import oracle.kv.impl.xregion.resolver.PrimaryKeyMetadata;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.util.PackedInteger;

/**
 * Server handler for {@link PutResolve}.
 *
 * Throughput calculation
 * +------------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        | * |     Write            |
 * |---------------+--------+---+----------------------+--------------------------|
 * |               |        | P |    old record size   | L | remote record size + |
 * |               |        |   |                      |   | local record size    |
 * |               |        |   |                      |--------------------------|
 * |               |        |   |                      | W |         0            |
 * |               |  NONE  |---+----------------------+--------------------------|
 * |               |        | A |           0          |    remote record size    |
 * |               +--------+---+----------------------+--------------------------|
 * |               |        | P |                      | L | remote record size + |
 * |               |        |   |    old record size   |   | local record size    |
 * |               |        |   |                      |--------------------------|
 * |               |        |   |                      | W |         0            |
 * | Put           | VERSION|---+----------------------+--------------------------|
 * |               |        | A |           0          |   remote record size     |
 * |               +--------+---+----------------------+--------------------------|
 * |               |        | P |                      | L | remote record size + |
 * |               |        |   |    old record size   |   | local record size    |
 * |               |        |   |                      |--------------------------|
 * |               |        |   |                      | W |         0            |
 * |               |  VALUE |---+----------------------+--------------------------|
 * |               |        | A |           0          |   remote record size     |
 * +------------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A) locally
 *      * = Local record wins (W) or losts (L) the conflict resolution.
 */
class PutResolveHandler extends BasicPutHandler<PutResolve> {

    PutResolveHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_RESOLVE, PutResolve.class);
    }

    @Override
    Result execute(PutResolve op,
                   Transaction txn,
                   PartitionId partitionId)
        throws UnauthorizedException {
        verifyDataAccess(op);

        byte[] keyBytes = op.getKeyBytes();
        byte[] valueBytes = op.getValueBytes();
        assert (keyBytes != null) && (valueBytes != null);

        PutHandler.checkTombstoneLength(op.isTombstone(), valueBytes.length);

        final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = valueDatabaseEntry(valueBytes);

        final WriteOptions writeOptions = getWriteOptions(op);
        writeOptions.setModificationTime(op.getTimestamp());

        final TableImpl table = getAndCheckTable(op.getTableId());
        if (table == null) {
            throw new FaultException(
                "Key/value request is not expected", true);
        }

        checkPutResolveRegion(table, op);

        /* check version of table and that in the op */
        versionCheck(op, table);

        final Database db = getRepNode().getPartitionDB(partitionId);
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        final boolean hasCRDT = getHasMRCounters(table);

        try {
            return put(cursor,
                       keyEntry,
                       dataEntry,
                       writeOptions,
                       op,
                       partitionId,
                       table,
                       hasCRDT);
        } finally {
            TxnUtil.close(cursor);
        }
    }

    /*
     * Check that the multi-region options and region ID in the PutResolve
     * operation are valid.
     */
    private void checkPutResolveRegion(TableImpl table, PutResolve op) {
        try {
            if (!table.isMultiRegion() && !op.isExternalMultiRegion()) {
                throw new IllegalArgumentException(
                    "PutResolve is not supported when the table and op are " +
                    "both not multi-region");
            }
            if (table.isMultiRegion() && op.isExternalMultiRegion()) {
                throw new IllegalArgumentException("Can't use external " +
                    "region when operate on multi-region table");
            }
            Region.checkId(op.getRemoteRegionId(), op.isExternalMultiRegion());
        } catch (IllegalArgumentException e) {
            throw new WrappedClientException(e);
        }
    }

    /**
     * Checks table version of table instance and that in the op, throw
     * exception if they do not match. There are possible cases
     * - delete op(or put tombstone), no version check. We're deleting the
     *   entry, and there is no table metadata if the entry is gone or replaced
     *   by a tombstone, so there is nothing to check.
     * - put op, both are key-only, no check because key-only values are empty
     *   so always identical, independent of the table version.
     * - put op, table is non-key-only, while op is key-only, throw exception
     * - all other cases, compare table version
     */
    private void versionCheck(PutResolve op,  TableImpl table) {
        if (op.isDelete() || op.isTombstone()) {
            /* no version check for deletion or tombstone */
            return;
        }

        final String tbName = table.getFullNamespaceName();
        final int serverVersion = table.getTableVersion();
        final int offset = op.computeOffset();
        if (op.keyOnlyPut(offset)) {
            if (table.isKeyOnly()) {
                /* both table and op are key-only */
                return;
            }
            /* table has evolved to be non-key-only on the server */
            final String err = "table=" + tbName +
                               " evolved to version=" + serverVersion +
                               ", which is non-key-only" +
                               ", client row is key-only op";
            throw new FaultException(
                err, new TargetTableEvolveException(), true);
        }

        /* a put of non-key-only table, compare versions from op and table */
        final int clientVersion = op.getTableVer(offset);
        /* check if table has evolved for put */
        if (clientVersion != 0 && serverVersion > clientVersion) {
            /* table has evolved on the server */
            final String err = "table=" + tbName +
                               " evolved to version=" + serverVersion +
                               ", client version=" + clientVersion;
            throw new FaultException(
                err, new TargetTableEvolveException(), true);
        }
    }

    private WriteOptions getWriteOptions(PutResolve op) {
        WriteOptions writeOptions;
        if (op.isTombstone()) {
            writeOptions =
                makeOption(getRepNode().getRepNodeParams().getTombstoneTTL(),
                           true);
            writeOptions.setTombstone(true);
        } else {
            writeOptions = makeExpirationTimeOption(op.getExpirationTimeMs(),
                                                    op.getUpdateTTL());
        }
        return writeOptions;
    }

    /*
     * NOTE: some of the same algorithms used here are used in
     * PutBatchHandler when usePutResolve is true. It may be possible to
     * refactor this code to share more. If changes to the algorithm are
     * made they may need to be made there as well
     */
    private Result.PutResult put(Cursor cursor,
                                 DatabaseEntry keyEntry,
                                 DatabaseEntry dataEntry,
                                 WriteOptions writeOptions,
                                 PutResolve op,
                                 PartitionId partitionId,
                                 TableImpl table,
                                 boolean hasCRDT) {
        ResultValueVersion prevVal = null;
        Version version = null;
        long expTime = 0L;
        long modificationTime = 0L;
        int storageSize = -1;
        boolean wasUpdate = false;

        while (true) {
            OperationResult opres = putEntry(cursor, keyEntry, dataEntry,
                                             NO_OVERWRITE, writeOptions);
            if (opres != null) {
                version = getVersion(cursor);
                expTime = opres.getExpirationTime();
                modificationTime = opres.getModificationTime();
                storageSize = getStorageSize(cursor);

                op.addReadBytes(MIN_READ);
                op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                 partitionId, storageSize);
            } else {
                final Choice choice = op.getReturnValueVersionChoice();
                final DatabaseEntry localDataEntry = new DatabaseEntry();

                opres = cursor.get(keyEntry, localDataEntry,
                                   Get.SEARCH, LockMode.RMW.toReadOptions());
                if (opres == null) {
                    /* Another thread deleted the record. Continue. */
                    continue;
                }

                /* set prevVal if needed*/
                prevVal = getBeforeUpdateInfo(choice, cursor,
                                              operationHandler,
                                              localDataEntry,
                                              opres);

                op.addReadBytes(getStorageSize(cursor));

                byte[] localValue = localDataEntry.getData();
                PrimaryKeyMetadata localKeyMeta =
                    getMRMeta(opres.getModificationTime(), localValue,
                              op.getLocalRegionId());
                PrimaryKeyMetadata remoteKeyMeta =
                    new PrimaryKeyMetadata(op.getTimestamp(),
                                           op.getRemoteRegionId());

                /* call resolver in table manager to resolve the conflict */
                final ConflictResolver resolver =
                    getRepNode().getTableManager().getResolver();
                final PrimaryKeyMetadata winner = (PrimaryKeyMetadata)
                    resolver.resolve(op.isExternalMultiRegion(),
                                     localKeyMeta,
                                     remoteKeyMeta);

                boolean remoteWin = winner.equals(remoteKeyMeta);

                if (hasCRDT && !opres.isTombstone() && !op.isTombstone()) {
                    /* Merge CRDTs. */
                    KVStoreImpl store = (KVStoreImpl)getRepNode().getKVStore();
                    RowImpl localRow = table.
                        createRowFromBytes(keyEntry.getData(),
                                           localValue,
                                           false/*keyOnly*/,
                                           true/*addMissingCol*/);
                    byte[] remoteValue = dataEntry.getData();
                    RowImpl remoteRow = table.
                        createRowFromBytes(keyEntry.getData(),
                                           remoteValue,
                                           false/*keyOnly*/,
                                           true/*addMissingCol*/);

                    if (remoteWin) {
                        /*
                         * The remote row wins, merge the CRDTs and overwrite
                         * other columns.
                         */
                        dataEntry = mergeCRDT(localRow, remoteRow, table, store,
                                              Value.Format.fromFirstByte(
                                                  remoteValue[0]));
                    } else {
                        /* Only merge the CRDTs. */
                        dataEntry = mergeCRDT(remoteRow, localRow, table, store,
                                              Value.Format.fromFirstByte(
                                                  localValue[0]));
                        /*
                         * The modification time should not be changed
                         * since except the CRDTs, other fields are not changed.
                         */
                        writeOptions.
                            setModificationTime(opres.getModificationTime());
                    }
                } else if (!remoteWin) {
                    /* No CRDTs and the local row wins, so should not
                     * update the value. */
                    dataEntry = null;
                }

                if (dataEntry != null) {
                    /* Charge for deletion of old value. */
                    final int oldRecordSize = getStorageSize(cursor);
                    op.addWriteBytes(oldRecordSize, 0,
                                     partitionId, -oldRecordSize);

                    opres = putEntry(cursor, null, dataEntry, Put.CURRENT,
                                     writeOptions);

                    version = getVersion(cursor);
                    expTime = opres.getExpirationTime();
                    wasUpdate = true;
                    modificationTime = opres.getModificationTime();
                    storageSize = getStorageSize(cursor);

                    op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                     partitionId, storageSize);
                }
                reserializeResultValue(op, prevVal);

            }

            if (version != null) {
                MigrationStreamHandle.get().addPut(keyEntry,
                                                   dataEntry,
                                                   version.getVLSN(),
                                                   modificationTime,
                                                   expTime,
                                                   op.isTombstone());
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
        }
    }

    private PrimaryKeyMetadata getMRMeta(long updateTime,
                                         byte[] valueBytes,
                                         int localRegionId) {

        /*
         * Local value can be empty value for tombstone put locally or value
         * with non MULTI_REGION_TABLE format.
         */
        Value.Format format = (valueBytes.length > 0) ?
                              Value.Format.fromFirstByte(valueBytes[0]) : null;
        if (format != Format.MULTI_REGION_TABLE) {
            if (Region.isMultiRegionId(localRegionId)) {
                /*
                 * If local row is not multi-region format, then it is a local
                 * change. We should use local region id as row region id.
                 */
                return new PrimaryKeyMetadata(updateTime, localRegionId);
            }
            throw new IllegalArgumentException("This is not a record of " +
                "multiregion tables.");
        }
        int regionId = PackedInteger.readInt(valueBytes, 1);
        return new PrimaryKeyMetadata(updateTime, regionId);
    }

    /*
     * Package protected for use by PutBatchHandler
     */
    static DatabaseEntry mergeCRDT(RowImpl sourceRow,
                                   RowImpl targetRow,
                                   TableImpl table,
                                   KVStoreImpl store,
                                   Value.Format valFormat) {

        RecordDefImpl rowDef = targetRow.getDefinition();

        for (int i = 0; i < rowDef.getNumFields(); ++i) {

            FieldDefImpl fdef = rowDef.getFieldDef(i);
            FieldValueImpl srcFieldVal = sourceRow.get(i);
            FieldValueImpl tarFieldVal = targetRow.get(i);

            if (fdef.isMRCounter()) {

                tarFieldVal.mergeMRCounter(srcFieldVal);

            } else if (fdef.hasJsonMRCounter()) {

                List<TablePath> mrcounterPaths =
                    table.getSchemaMRCounterPaths(i);

                for (TablePath path : mrcounterPaths) {

                    FieldValueImpl srcVal = sourceRow.
                                             evaluateScalarPath(path, 0);
                    FieldValueImpl destVal = targetRow.
                                             evaluateScalarPath(path, 0);

                    destVal.mergeMRCounter(srcVal);
                }

            }
        }

        Value value = table.createValueInternal(targetRow,
                                                valFormat,
                                                targetRow.getRegionId(),
                                                store,
                                                null    /* genInfo */,
                                                false   /* replaceCRDT */);
        return valueDatabaseEntry(value.toByteArray());
    }
}
