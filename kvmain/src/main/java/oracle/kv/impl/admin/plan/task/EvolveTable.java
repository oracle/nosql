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

package oracle.kv.impl.admin.plan.task;

import static oracle.kv.impl.admin.plan.task.AddTable.tableMetadataNotFound;

import java.util.Set;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Version;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.TableNotFoundException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.IdentityColumnInfo;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.SequenceImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.systables.SGAttributesTableDesc;
import oracle.kv.impl.systables.SGAttributesTableDesc.SGType;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.LongValue;
import oracle.kv.table.NumberValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import com.sleepycat.je.Transaction;

/**
 * Evolve a table
 */
public class EvolveTable extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String tableName;
    private final int tableVersion;
    private final FieldMap fieldMap;
    private final TimeToLive ttl;
    private final TimeToLive beforeImgTTL;
    private final String description;
    private final boolean systemTable;
    private final IdentityColumnInfo identityColumnInfo;
    private final Set<Integer> regions;

    /*
     * The table id of the target table when the task was created. It may be
     * 0 if the task was deserialized from an earlier version.
     */
    private final long tableId;

    /* If there is no change to the sequence definition this will be null */
    private final SequenceDef sequenceDefChange;

    public static final int MAX_SG_TRIES = 1000;

    /**
     */
    public EvolveTable(MetadataPlan<TableMetadata> plan,
                       String namespace,
                       String tableName,
                       int tableVersion,
                       FieldMap fieldMap,
                       TimeToLive ttl,
                       TimeToLive beforeImgTTL,
                       String description,
                       boolean systemTable,
                       IdentityColumnInfo identityColumnInfo,
                       SequenceDef sequenceDefChange,
                       Set<Integer> regions) {
        super(plan);

        /*
         * Caller verifies parameters
         */
        this.tableName = tableName;
        this.namespace = namespace;
        this.fieldMap = fieldMap;
        this.tableVersion = tableVersion;
        this.ttl = ttl;
        this.beforeImgTTL = beforeImgTTL;
        this.description = description;
        this.systemTable = systemTable;
        this.identityColumnInfo = identityColumnInfo;

        final TableMetadata md = getMetadata();
        if (md == null) {
            throw tableMetadataNotFound();
        }
        final TableImpl table = md.getTable(namespace, tableName);
        if (table == null) {
            throw tableDoesNotExist(namespace, tableName);
        }
        tableId = table.getId();
        this.sequenceDefChange = sequenceDefChange;
        if (identityColumnInfo != null) {
            Type dataType = fieldMap.getFieldDef(
                identityColumnInfo.getIdentityColumn()).getType();
            if (dataType != Type.INTEGER && dataType != Type.LONG &&
                dataType != Type.NUMBER) {
              throw new IllegalArgumentException(
                  "Identity field must be one of the following numeric " +
                  "datatypes: INTEGER, LONG or NUMBER.");
            }
        }
        this.regions = regions;
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockTable(planner, getPlan(), namespace, tableName);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        /*
         * The table could have been removed and recreated since the task was
         * created. Note that tableId could be 0 if the task was from a
         * previous version. Skip the check if so.
         */
        final TableImpl table = md.getTable(namespace, tableName);
        if ((table == null) ||                                  /* gone */
            table.isDeleting() ||                               /* going */
            ((tableId != 0) && (tableId != table.getId()))) {   /* returned */
            throw tableDoesNotExist(namespace, tableName);
        }

        boolean oldTableHasIdentity = table.hasIdentityColumn();
        int oldTableIdentityColumn = -1;
        String oldSgName = null;
        if (oldTableHasIdentity) {
            oldTableIdentityColumn = table.getIdentityColumn();
            oldSgName = SequenceImpl.getSgName(table, oldTableIdentityColumn);

        }
        final Admin admin = getPlan().getAdmin();
        final KVStoreImpl store = (KVStoreImpl)admin.getInternalKVStore();

        /* From this point the table is evolved */
        if (md.evolveTable(table, tableVersion, fieldMap, ttl, beforeImgTTL,
                           description, systemTable, identityColumnInfo,
                           regions)) {
            if (!oldTableHasIdentity && table.hasIdentityColumn() &&
                sequenceDefChange != null) {
                addIdentityColumn(table, store);
            }
            admin.saveMetadata(md, txn);

            if (oldTableHasIdentity && !table.hasIdentityColumn()) {
                deleteSequence(SGType.INTERNAL,
                               oldSgName,
                               store);
            }

            /* If this is a multi-region table, notify the MRT service. */
            if (table.isMultiRegion()) {
                admin.getMRTServiceManager()
                    .postUpdateMRT(getPlan().getId(), table.getTopLevelTable(),
                                   md.getSequenceNumber());
            }
        }

        if (oldTableHasIdentity && table.hasIdentityColumn() &&
            sequenceDefChange != null) {
            if (oldTableIdentityColumn != table.getIdentityColumn()) {
                throw new IllegalArgumentException("Only one identity column " +
                    "is allowed in a table.");
            }
            evolveIdentityColumn(table, store);
        }

        /*
         *  Persist in DB now, so the update to the MD system
         *  is matches the DB.
         */
        TxnUtil.localCommit(txn, admin.getLogger());

        /*
         * The table may or may not have been updated. Call updateTable
         * to update the MD system table if needed.
         */
        MDTableUtil.updateTable(table, md, admin);
        return md;
    }

    private void addIdentityColumn(TableImpl table, KVStoreImpl store) {
        int idColumn = identityColumnInfo.getIdentityColumn();
        Type idtype = table.getRowDef().getFieldDef(idColumn).getType();
        addSequence(SGType.INTERNAL,
                    SequenceImpl.getSgName(table, idColumn),
                    idtype,
            sequenceDefChange, store);
    }

    private void evolveIdentityColumn(TableImpl table, KVStoreImpl store) {
        int idColumn = identityColumnInfo.getIdentityColumn();
        Type idtype = table.getRowDef().getFieldDef(idColumn).getType();
        alterSequence(SGType.INTERNAL, SequenceImpl.getSgName(table, idColumn),
                      idtype, sequenceDefChange, store);
    }


    static TableNotFoundException tableDoesNotExist(String namespace,
                                                    String tableName) {
        return new TableNotFoundException(
                        "Table " +
                        NameUtils.makeQualifiedName(namespace, tableName) +
                        " does not exist");
    }

    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (t == null) {
            return false;
        }

        if (getClass() != t.getClass()) {
            return false;
        }

        EvolveTable other = (EvolveTable) t;

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return (tableName.equalsIgnoreCase(other.tableName) &&
                (tableVersion == other.tableVersion)  &&
                (fieldMap.equals(other.fieldMap)));
    }

    static void alterSequence(SGType sgType,
                              String sgName,
                              Type dataType,
                              SequenceDef sequenceDef,
                              KVStoreImpl store) {
        TableAPI tableAPI = store.getTableAPI();
        Table sysTable = tableAPI.
            getTable(SGAttributesTableDesc.TABLE_NAME);

        PrimaryKey pk = sysTable.createPrimaryKey();

        pk.put(SGAttributesTableDesc.COL_NAME_SGTYPE, sgType.toString());
        pk.put(SGAttributesTableDesc.COL_NAME_SGNAME, sgName);

        /* get previous attributes */
        Row sysRow = tableAPI.get(pk,
            new ReadOptions(Consistency.ABSOLUTE, 0, null));

        if (sysRow == null) {
            throw new IllegalArgumentException("Sequence not found to " +
                "alter: " + sgName);
        }


        for (int i = 0; i < MAX_SG_TRIES; i++) {
            Version oldVersion = sysRow.getVersion();
            sysRow.put(SGAttributesTableDesc.COL_NAME_DATATYPE,
                dataType.toString());
            if (sequenceDef.isSetCycle()) {
                sysRow.put(SGAttributesTableDesc.COL_NAME_CYCLE,
                    sequenceDef.getCycle());
            }
            /* increment the SGAttrVersion so that clients will know to
             * update */
            sysRow.put(SGAttributesTableDesc.COL_NAME_VERSION,
                FieldValueFactory.createLong(
                sysRow.get(SGAttributesTableDesc.COL_NAME_VERSION).
                asLong().get() + 1));

            NumberValue startWith = null;
            NumberValue minValue = null;
            NumberValue maxValue = null;
            LongValue incrementBy = null;
            LongValue sgCache = null;
            /* Create the data type values according the specified
             * data type */
            switch (dataType) {
            case INTEGER:
                startWith = FieldValueFactory.
                    createNumber(sequenceDef.getStartValue().asInteger().
                                 get());

                minValue = FieldValueFactory.
                    createNumber(sequenceDef.getMinValue().asInteger().
                                 get());

                maxValue = FieldValueFactory.
                    createNumber(sequenceDef.getMaxValue().asInteger().
                                 get());

                incrementBy = FieldValueFactory.
                    createLong(
                        sequenceDef.getIncrementValue().asInteger().
                        get());

                sgCache = FieldValueFactory.
                    createLong(sequenceDef.getCacheValue().asInteger().
                               get());
                break;

            case LONG:
                startWith = FieldValueFactory.
                    createNumber(sequenceDef.getStartValue().asLong().
                                 get());

                minValue = FieldValueFactory.
                    createNumber(sequenceDef.getMinValue().asLong().
                                 get());

                maxValue = FieldValueFactory.
                    createNumber(sequenceDef.getMaxValue().asLong().get());

                incrementBy = sequenceDef.getIncrementValue().asLong();

                sgCache = sequenceDef.getCacheValue().asLong();
                break;

            case NUMBER:
                startWith = sequenceDef.getStartValue().asNumber();

                minValue = sequenceDef.getMinValue() == null ?
                    null : sequenceDef.getMinValue().asNumber();

                maxValue = sequenceDef.getMaxValue() == null ?
                    null : sequenceDef.getMaxValue().asNumber();

                incrementBy = FieldValueFactory.
                    createLong(sequenceDef.getIncrementValue().asNumber().
                               get().longValue());

                sgCache = FieldValueFactory.
                    createLong(sequenceDef.getCacheValue().asNumber().get().
                        longValue());
                break;

            default:
                throw new IllegalArgumentException(
                    "Identity field must be one of the following numeric " +
                        "datatypes: INTEGER, LONG or NUMBER.");
            }

            if (sequenceDef.isSetStartValue()) {
                sysRow.put(SGAttributesTableDesc.COL_NAME_STARTWITH,
                           startWith);
                sysRow.put(SGAttributesTableDesc.COL_NAME_CURRENTVALUE,
                           startWith);
            }

            if (sequenceDef.isSetMinValue() && minValue != null) {
                sysRow.put(SGAttributesTableDesc.COL_NAME_MINVALUE,
                           minValue);
            }

            if (sequenceDef.isSetMaxValue() && maxValue != null) {
                sysRow.put(SGAttributesTableDesc.COL_NAME_MAXVALUE,
                           maxValue);
            }

            if (sequenceDef.isSetIncrementValue()) {
                sysRow.put(SGAttributesTableDesc.COL_NAME_INCREMENTBY,
                    incrementBy);
            }

            if (sequenceDef.isSetCacheValue()) {
                sysRow.put(SGAttributesTableDesc.COL_NAME_CACHE, sgCache);
            }

            ReturnRow previousRow = sysTable
                .createReturnRow(ReturnRow.Choice.ALL);
            Version v = tableAPI.putIfVersion(sysRow, oldVersion,
                previousRow, new WriteOptions(Durability.COMMIT_SYNC,
                                              0, null));

            if (v != null) {
                /* write was successful */
                return;
            }

            sysRow = previousRow.asRow();
        }
        throw new IllegalStateException(
            "Reached max number of retries: " + sgName);

    }

    static void addSequence(SGType sgType,
                                String sgName,
                                Type dataType,
                                SequenceDef sequenceDef,
                                KVStoreImpl store) {
        TableAPI tableAPI = store.getTableAPI();

        Table sysTable = tableAPI.
            getTable(SGAttributesTableDesc.TABLE_NAME);
        long version = 0;
        Row sysRow = sysTable.createRow();
        sysRow.put(SGAttributesTableDesc.COL_NAME_SGTYPE,
                   sgType.toString());
        sysRow.put(SGAttributesTableDesc.COL_NAME_SGNAME, sgName);

        sysRow.put(SGAttributesTableDesc.COL_NAME_DATATYPE,
                   dataType.toString());

        sysRow.put(SGAttributesTableDesc.COL_NAME_CYCLE,
                   sequenceDef.getCycle());
        sysRow.put(SGAttributesTableDesc.COL_NAME_VERSION,
                   FieldValueFactory.createLong(0));

        NumberValue startWith = null;
        NumberValue minValue = null;
        NumberValue maxValue = null;
        NumberValue currentValue = null;
        LongValue incrementBy = null;
        LongValue sgCache = null;
        /* Create the data type values according the specified data type */
        switch (dataType) {
        case INTEGER:
            startWith = FieldValueFactory.
                createNumber(sequenceDef.getStartValue().asInteger().get());

            minValue = FieldValueFactory.
                    createNumber(sequenceDef.getMinValue().asInteger().
                                 get());

            maxValue = FieldValueFactory.
                    createNumber(sequenceDef.getMaxValue().asInteger().
                                 get());

            currentValue = startWith;

            incrementBy = FieldValueFactory.
                createLong(sequenceDef.getIncrementValue().asInteger().
                           get());

            sgCache = FieldValueFactory.
                createLong(sequenceDef.getCacheValue().asInteger().get());
            break;

        case LONG:
            startWith = FieldValueFactory.
                createNumber(sequenceDef.getStartValue().asLong().get());

            minValue = FieldValueFactory.
                createNumber(sequenceDef.getMinValue().asLong().get());

            maxValue = FieldValueFactory.
                createNumber(sequenceDef.getMaxValue().asLong().get());

            currentValue = startWith;

            incrementBy = sequenceDef.getIncrementValue().asLong();

            sgCache = sequenceDef.getCacheValue().asLong();
            break;

        case NUMBER:
            startWith = sequenceDef.getStartValue().asNumber();

            minValue = sequenceDef.getMinValue() == null ?
                null : sequenceDef.getMinValue().asNumber();

            maxValue = sequenceDef.getMaxValue() == null ?
                null : sequenceDef.getMaxValue().asNumber();

            currentValue = startWith;

            incrementBy = FieldValueFactory.
                createLong(sequenceDef.getIncrementValue().asNumber().get().
                           longValue());
            sgCache = FieldValueFactory.
                createLong(sequenceDef.getCacheValue().asNumber().get().
                           longValue());
            break;

        default:
            throw new IllegalArgumentException(
                "Identity field must be one of the following numeric " +
                "datatypes: INTEGER, LONG or NUMBER.");
        }

        sysRow.put(SGAttributesTableDesc.COL_NAME_STARTWITH, startWith);
        if (minValue != null) {
            sysRow.put(SGAttributesTableDesc.COL_NAME_MINVALUE, minValue);
        }

        if (maxValue != null) {
            sysRow.put(SGAttributesTableDesc.COL_NAME_MAXVALUE, maxValue);
        }

        sysRow.put(SGAttributesTableDesc.COL_NAME_CURRENTVALUE,
                   currentValue);
        sysRow.put(SGAttributesTableDesc.COL_NAME_INCREMENTBY, incrementBy);
        sysRow.put(SGAttributesTableDesc.COL_NAME_CACHE, sgCache);
        sysRow.put(SGAttributesTableDesc.COL_NAME_VERSION, version);

        tableAPI.putIfAbsent(sysRow, null /* previous-row */,
                        new WriteOptions(Durability.COMMIT_SYNC, 0, null));
    }

    static void deleteSequence(SGType sgType,
                               String sgName,
                               KVStoreImpl store) {
        TableAPI tableAPI = store.getTableAPI();
        Table sysTable = tableAPI.
            getTable(SGAttributesTableDesc.TABLE_NAME);
        PrimaryKey pk = sysTable.createPrimaryKey();

        pk.put(SGAttributesTableDesc.COL_NAME_SGTYPE, sgType.toString());
        pk.put(SGAttributesTableDesc.COL_NAME_SGNAME, sgName);

        tableAPI.delete(pk, null /* previous-row */,
                        new WriteOptions(Durability.COMMIT_SYNC, 0, null));

    }
}
