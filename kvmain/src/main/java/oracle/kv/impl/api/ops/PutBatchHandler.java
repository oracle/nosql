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
import static oracle.kv.impl.security.KVStorePrivilegeLabel.DELETE_TABLE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.UnauthorizedException;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.bulk.BulkPut.KVPair;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler.KVAuthorizer;
import oracle.kv.impl.api.ops.Result.PutBatchResult;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.WriteOptions;

/**
 * Server handler for {@link PutBatch}.
 *
 * Throughput calculation
 * +---------------------------------------------------------------------------+
 * |    Op         | Choice | # |          Read        |       Write           |
 * |---------------+--------+---+----------------------+-----------------------|
 * |               |        | P | if usePutResolve, old| if overwrite, new rec |
 * |               |        |   | record size, else 0  | size, else 0          |
 * | PutBatch (1)  |   N/A  |---+----------------------+-----------------------|
 * |               |        | A |           0          |    new record size    |
 * +---------------------------------------------------------------------------+
 *      # = Target record is present (P) or absent (A)
 *      (1) The calculation entry is per input record. The total throughput
 *          is the sum of the result from each input record.
 */
class PutBatchHandler extends MultiKeyOperationHandler<PutBatch> {

    Set<Long> tablesWithCRDT = new HashSet<>();

    PutBatchHandler(OperationHandler handler) {
        super(handler, OpCode.PUT_BATCH, PutBatch.class);
    }

    @Override
    Result execute(PutBatch op,
                   Transaction txn,
                   PartitionId partitionId)
        throws UnauthorizedException {

        checkTableExists(op);

        final KVAuthorizer kvAuth = checkPermission(op);

        final List<Integer> keysPresent =
            putBatch(op, txn, partitionId, op.getKvPairs(), kvAuth);

        return new PutBatchResult(op.getReadKB(), op.getWriteKB(),
                                  op.getKvPairs().size(), keysPresent);
    }

    private List<Integer> putBatch(PutBatch op,
                                   Transaction txn,
                                   PartitionId partitionId,
                                   List<KVPair> kvPairs,
                                   KVAuthorizer kvAuth) {

        final WriteOptions noExpiry =
            makeOption(TimeToLive.DO_NOT_EXPIRE, false, op.getTableId(),
                       getOperationHandler(), false/* not tombstone */);

        final List<Integer> keysPresent = new ArrayList<>();

        final Database db = getRepNode().getPartitionDB(partitionId);
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();

        /*
         * To return previous value/version, we have to either position on the
         * existing record and update it, or insert without overwriting.
         */
        final Cursor cursor = db.openCursor(txn, CURSOR_DEFAULT);
        int i = -1;
        byte[] lastKey = null;
        final Put put = op.getOverwrite() ? Put.OVERWRITE : Put.NO_OVERWRITE;

        try {
            for (KVPair e : kvPairs) {
                i++;
                if (!op.getOverwrite()) {
                    if (lastKey != null && Arrays.equals(lastKey, e.getKey())) {
                        keysPresent.add(i);
                        continue;
                    }
                }
                lastKey = e.getKey();

                keyEntry.setData(e.getKey());
                /*
                 * The returned entry may be the same one passed in, but if the
                 * entry is empty, it'll be a static, shared value.
                 */
                DatabaseEntry dataEntryToUse =
                    valueDatabaseEntry(dataEntry, e.getValue());

                if (!kvAuth.allowAccess(keyEntry, getTablePrivileges(e))) {
                    throw new UnauthorizedException("Insufficient access " +
                      "rights granted");
                }

                if (op.getUsePutResolve()) {
                    /*
                     * PutResolve semantics are different enough to use a
                     * separate path
                     */
                    handlePutResolve(cursor, e, keyEntry, dataEntryToUse,
                                     op, partitionId);
                    continue;
                }

                /*
                 * For tables wtih CRDTs, check if there is
                 * an existing row with the key.
                 */
                DatabaseEntry prevData = null;
                boolean replaceCRDT = false;
                TableImpl table = null;
                if (tablesWithCRDT.size() > 0) {
                    table = findTable(op, e.getKey());
                    if (op.getOverwrite() && table != null &&
                        tablesWithCRDT.contains(table.getId())) {
                        prevData  = new DatabaseEntry();
                        OperationResult opres = cursor.get(keyEntry, prevData,
                            Get.SEARCH,
                            InternalOperationHandler.RMW_EXCLUDE_TOMBSTONES);
                        if (opres != null) {
                            replaceCRDT = true;
                        }
                    }
                }


                while (true) {
                    final WriteOptions jeOptions;
                    int ttlVal = e.getTTLVal();
                    if (ttlVal != 0) {
                        jeOptions = makeJEWriteOptions(ttlVal, e.getTTLUnit());
                    } else {
                        jeOptions = noExpiry;
                    }

                    /* set before image TTL */
                    final TableImpl tb = findTable(op, e.getKey());
                    if (tb != null) {
                        operationHandler.setBeforeImageTTL(jeOptions,
                                                           tb.getId());
                    }

                    if (replaceCRDT) {
                        dataEntryToUse = PutHandler.copyCRDTFromPrevRow(table,
                            e.getKey(), e.getValue(), prevData, getRepNode());
                    }

                    final OperationResult result =
                            BasicPutHandler.putEntry(cursor, keyEntry,
                                                     dataEntryToUse,
                                                     put,
                                                     jeOptions);
                    if (result != null) {
                        final int storageSize = getStorageSize(cursor);
                        op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                         partitionId, storageSize);
                        final Version v = getVersion(cursor);
                        MigrationStreamHandle.get().
                            addPut(keyEntry, dataEntryToUse,
                                   v.getVLSN(),
                                   result.getCreationTime(),
                                   result.getModificationTime(),
                                   result.getExpirationTime(),
                                   false /*isTombstone*/);
                        break;
                    }
                    /* Key already exists. */
                    keysPresent.add(i);
                    break;
                }
            }
        } finally {
            TxnUtil.close(cursor);
        }

        return keysPresent;
    }

    /**
     * Get the table using tableId if the op is against only one table,
     * otherwise find the table by key bytes.
     */
    private TableImpl findTable(PutBatch op, byte[] keyBytes) {
        final long[] tableIds = op.getTableIds();
        if (tableIds == null) {
            return null;
        }
        return tableIds.length > 1 ?
            findTableByKeyBytes(keyBytes) : getAndCheckTable(tableIds[0]);
    }

    /*
     * Use PutResolve semantics. This path handles both true MR merging
     * as well as simple restore from backup. This is doing similar work
     * to PutResolveHandler but it would be tricky, and risky to refactor
     * that code to make it shareable. Perhaps that should be done at
     * some point.
     *
     * In this path set modification time, obtained from KVPair
     * 1. Use PUT.NO_OVERWRITE. If it succeeds, done
     * 2. Read existing (local) value. Include Tombstones.
     * 3. Choose winner, local vs remote
     * 4. If merging CRDTs and the table has CRDT, merge them based
     *    on the winner/loser
     * 5. write the winner
     */
    private void handlePutResolve(Cursor cursor, KVPair e,
                                  DatabaseEntry keyEntry,
                                  DatabaseEntry dataEntry,
                                  PutBatch op,
                                  PartitionId partitionId) {

        /* state for migration stream */
        Version version = null;
        long creationTime = 0l;
        long modTime = 0l;
        long expiration = 0L;

        com.sleepycat.je.WriteOptions jeOptions = null;


        final long tableId = op.getTableId();
        final OperationHandler opHandler = getOperationHandler();
        final boolean tombstone = e.isTombstone();
        final TimeToLive ttl;

        if (e.isTombstone()) {
            ttl = getRepNode().getRepNodeParams().getTombstoneTTL();
        } else if (e.getTTLVal() != 0) {
            /* using overwrite, update the TTL if it's set */
            ttl = TimeToLive.createTimeToLive(e.getTTLVal(), e.getTTLUnit());
        } else {
            ttl = TimeToLive.DO_NOT_EXPIRE;
        }
        /* create JE write option */
        jeOptions = makeOption(ttl, true, tableId, opHandler, tombstone);

        /* if creationTime is 0 it's the same as the default */
        jeOptions.setCreationTime(e.getCreationTime());

        /* if mod time is 0 it's the same as the default */
        jeOptions.setModificationTime(e.getModificationTime());

        OperationResult opres =
            BasicPutHandler.putEntry(cursor, keyEntry, dataEntry,
                                     Put.NO_OVERWRITE, jeOptions);
        if (opres != null) {
            /*
             * No conflicts, save state for migration stream
             */
            version = getVersion(cursor);
            expiration = opres.getExpirationTime();
            creationTime = opres.getCreationTime();
            modTime = opres.getModificationTime();

            final int storageSize = getStorageSize(cursor);
            op.addReadBytes(MIN_READ); /* no overwrite put does a read */
            op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                             partitionId, storageSize);
        } else {
            /* read the local row. This will read tombstones */
            final DatabaseEntry localDataEntry = new DatabaseEntry();
            opres = cursor.get(keyEntry, localDataEntry,
                               Get.SEARCH, LockMode.RMW.toReadOptions());
            if (opres == null) {
                /* a race -- the record has been deleted */
                return;
            }

            long localModTime = opres.getModificationTime();
            long remoteModTime = e.getModificationTime();

            op.addReadBytes(getStorageSize(cursor));

            /*
             * choose a winner, last write, using larger region id as fallback.
             * Region ids come from (1) the row being put (remote) and
             * (2) the existing row (local row). If either is not in M-R
             * format that means it's a "local" row so use the region ID
             * passed in the PutBatch itself.
             * NOTE: check about using region name vs id for resolve. PutResolve
             * does this, depending on conditions
             */
            boolean remoteWin;
            if (remoteModTime == localModTime) {
                int localRegionId = getRegionId(localDataEntry,
                                                op.getLocalRegionId());
                int remoteRegionId = getRegionId(dataEntry,
                                                 op.getLocalRegionId());
                /*
                 * 2 cases:
                 *  1. use region ids to break tie if the region id has been
                 *  set in the request (op.isExternalMultiRegion()).
                 *  This is used by the cloud GAT or other entities that might
                 *  use coordinated region ids and not string region names
                 *  2. use region names to break tie if this is an onprem call
                 *  where region names are coordinated across regions, but ids
                 *  are not
                 */
                if (op.isExternalMultiRegion()) {
                    /*
                     * resolution in PutResolve in this case has the region
                     * region id that's <= winning the resolution
                     */
                    remoteWin = remoteRegionId <= localRegionId;
                } else if (localRegionId == 0 || remoteRegionId == 0) {
                    /*
                     * This is a put resolve without 2 valid regions and
                     * the modification times are the same, use the remote
                     * as winner. This case can happen in a non-MR situation
                     * such as restore or in testing. In real MR paths region
                     * ids will exist and be valid
                     */
                    remoteWin = true;
                } else {
                    remoteWin = breakTieByRegionName(localRegionId,
                                                     remoteRegionId);
                }
            } else {
                /* latest mod time wins */
                remoteWin = remoteModTime > localModTime;
            }

            /*
             * If either local or remote is a tombstone there is nothing
             * to merge, just write the winner
             */
            if (tablesWithCRDT.size() > 0 &&
                !opres.isTombstone() &&
                !e.isTombstone()) {
                /*
                 * Merge MR counters if present
                 *  1. does this table have MR counters?
                 *  2. If has MR counters:
                 *     If remote won, merge local counters into it
                 *     If local won, merge remote counters and reset the
                 *     mod time to the local one since it's not really changing
                 *  3. set dataEntry based on winner and write it
                 */
                TableImpl table = findTableByKeyBytes(keyEntry.getData());
                if (tablesWithCRDT.contains(table.getId())) {
                    /* need to merge MR counters, create RowImpl for both */
                    RowImpl localRow = table.
                        createRowFromBytes(keyEntry.getData(),
                                           localDataEntry.getData(),
                                           false, true);
                    RowImpl remoteRow = table.
                        createRowFromBytes(keyEntry.getData(),
                                           dataEntry.getData(),
                                           false, true);
                    KVStoreImpl store = (KVStoreImpl)getRepNode().getKVStore();
                    if (remoteWin) {
                        /* merge counters into the new row */
                        dataEntry = PutResolveHandler.
                            mergeCRDT(localRow, remoteRow, table, store,
                                      Value.Format.fromFirstByte(
                                          dataEntry.getData()[0]));
                    } else {
                        /* only merge new counters into existing row */
                        dataEntry = PutResolveHandler.
                            mergeCRDT(remoteRow, localRow, table, store,
                                      Value.Format.fromFirstByte(
                                          localDataEntry.getData()[0]));

                        /* use existing creation time */
                        jeOptions.setCreationTime(opres.getCreationTime());
                        /* use existing mod time */
                        jeOptions.
                            setModificationTime(opres.getModificationTime());
                    }
                } else if (!remoteWin) {
                    dataEntry = null; /* local win, no merge, done */
                }

            } else if (!remoteWin) {
                dataEntry = null; /* local win, no merge, done */
            }

            if (dataEntry != null) {
                /* Charge for deletion of old value. */
                final int oldRecordSize = getStorageSize(cursor);
                op.addWriteBytes(oldRecordSize, 0,
                                 partitionId, -oldRecordSize);

                opres = BasicPutHandler.putEntry(cursor, null, dataEntry,
                                                 Put.CURRENT, jeOptions);
                version = getVersion(cursor);
                expiration = opres.getExpirationTime();
                creationTime = opres.getCreationTime();
                modTime = opres.getModificationTime();
                final int storageSize = getStorageSize(cursor);
                op.addWriteBytes(storageSize, getNIndexWrites(cursor),
                                 partitionId, storageSize);
            }
        }
        /* add to migration stream */
        if (version != null) {
            MigrationStreamHandle.get().addPut(keyEntry,
                                               dataEntry,
                                               version.getVLSN(),
                                               creationTime,
                                               modTime,
                                               expiration,
                                               e.isTombstone());
        }
    }

    /*
     * Gets region id if one exists otherwise return localRegionId.
     */
    private static int getRegionId(DatabaseEntry entry, int localRegionId) {
        byte[] valueBytes = entry.getData();
        int regionId = Value.getRegionIdFromByteArray(valueBytes);
        if (regionId == Region.NULL_REGION_ID) {
            return localRegionId;
        }
        return regionId;
    }

    /*
     * See caller above. Return true if the remote region wins because the
     * remote region name is greater than the local one based on
     * case-insensitive string comparison. See LastWinWriteResolver
     */
    private boolean breakTieByRegionName(int localRegionId,
                                         int remoteRegionId) {
        TableMetadata tm = getRepNode().getTableManager().getTableMetadata();
        final String lregion = tm.getRegionName(localRegionId);
        final String rregion = tm.getRegionName(remoteRegionId);
        return rregion.compareToIgnoreCase(lregion) >= 0;
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(PutBatch op) {
        /*
         * Checks the basic privilege for authentication here, and leave the
         * keyspace checking and the table access checking in
         * {@code operationHandler.putIfAbsentBatch()}.
         */
        return SystemPrivilege.usrviewPrivList;
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
    public
    List<? extends KVStorePrivilege> tableAccessPrivileges(long tableId) {
        return Collections.singletonList(
                   new TablePrivilege.InsertTable(tableId));
    }

    @Override
    public List<? extends KVStorePrivilege>
    namespaceAccessPrivileges(String namespace) {
        return Collections.singletonList(
            new NamespacePrivilege.InsertInNamespace(namespace));
    }

    private void checkTableExists(PutBatch op) {
        if (op.getTableIds() != null) {
            for (long id : op.getTableIds()) {
                TableImpl table = getAndCheckTable(id);
                if (table != null) {
                    if (BasicPutHandler.getHasMRCounters(table)) {
                        tablesWithCRDT.add(table.getId());
                    }
                }
            }
        }
    }

    private com.sleepycat.je.WriteOptions makeJEWriteOptions(
        int ttlVal, TimeUnit ttlUnit) {

        return new com.sleepycat.je.WriteOptions()
            .setTTL(ttlVal, ttlUnit)
            .setUpdateTTL(false);
    }

    /*
     * Get additional table privileges required to perform this put operation.
     *
     * When an operation has a valid TTL, it is an implicit delete and
     * requires DELETE_TABLE privilege.
     */
    private EnumSet<KVStorePrivilegeLabel> getTablePrivileges(KVPair kvPair) {
        if (kvPair.getTTLVal() != 0) {
            return EnumSet.of(DELETE_TABLE);
        }
        return null;
    }
}
