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

package oracle.kv.impl.rep.migration;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import oracle.kv.KVVersion;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrations.MigrationRecord;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;

/**
 * Container class for partition migration records.
 */
class PartitionMigrations implements Iterable<MigrationRecord>, Serializable  {
    private static final long serialVersionUID = 1L;

    static final String MIGRATION_DB_NAME = "MigrationDB";

    private static final String PARTITION_MIGRATIONS_KEY ="PartitionMigrations";

    private final static DatabaseEntry migrationsKey = new DatabaseEntry();

    static {
        StringBinding.stringToEntry(PARTITION_MIGRATIONS_KEY, migrationsKey);
    }

    private static final int CURRENT_SCHEMA_VERSION = 1;

    /* Marker to prevent an object fetched for read-only from being persisted */
    private static final int READ_ONLY_VERSION = -1;

    /*
     * Transaction config to return what is locally at the replica, without
     * waiting for the master to make the state consistent wrt the master.
     * Also, the transaction will not wait for commit acks from the replicas.
     */
    static private final TransactionConfig NO_WAIT_CONFIG =
        new TransactionConfig().
            setDurability(new Durability(SyncPolicy.NO_SYNC,
                                         SyncPolicy.NO_SYNC,
                                         ReplicaAckPolicy.NONE)).
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

    private final Map<PartitionId, MigrationRecord> records = new HashMap<>();

    /*
     * The change number of the object.
     *
     * Whenever a change is made to a partition migration record that affects
     * the local topology the changeNumber must be incremented. This will cause
     * replicas to update their local topology when they receive the trigger.
     * The master updates the local topology in-line with the write operation.
     * The operations that affect the local topology are: the source completes
     * sending data (EOD), when the target completes processing that data, and
     * when a migration is canceled (when its record removed).
     */
    private long changeNumber = 0;

    /*
     * The sequence number of the last topology seen.
     *
     * A partition migration is completed when the admin sends out a new
     * version of the topology which includes the partition's new locations.
     * When this happens the source partition DB and the migration record can be
     * removed. When the migration record is removed the sequence number of the
     * new topology is recorded in topoSequenceNum. From then on, the replicas
     * can only update a topology with greater than or equal sequence
     * number. This is necessary to prevent the source replica from opening
     * the old partition DB because an old topology will have the partition at
     * its old location and there will be no migration record to redirect it.
     */
    private int topoSequenceNum = 0;

    /*
     * Generator for migration record ID.
     */
    private long nextRecordId = 1;

    /*
     * Version of this object. Set to READ_ONLY_VERSION if this is a read-only
     * copy that should not be persisted.
     */
    private int version;

    private PartitionMigrations() {
        version = CURRENT_SCHEMA_VERSION;
    }

    /**
     * Gets the partition migration object change number.
     *
     * @return the partition migration object change number
     */
    long getChangeNumber() {
        return changeNumber;
    }

    /**
     * Gets the last seen topology sequence number.
     *
     * @return the last seen topology sequence number
     */
    int getTopoSequenceNum() {
        return topoSequenceNum;
    }

    /**
     * Sets the last seen topology sequence number.
     *
     * @param seqNum
     */
    void setTopoSequenceNum(int seqNum) {
        assert seqNum >= topoSequenceNum;
        topoSequenceNum = seqNum;
    }

    /**
     * Adds the specified record. If a record with the same partition ID is
     * present it is replaced.
     *
     * @param record migration record to add
     */
    void add(MigrationRecord record) {
        records.put(record.partitionId, record);
    }

    /**
     * Gets any migration record associated with the specified partition.
     * Returns null if no record is found.
     *
     * @param partitionId a partition Id
     * @return a migration record or null
     */
    MigrationRecord get(PartitionId partitionId) {
        return records.get(partitionId);
    }

    /**
     * Gets the target migration record associated with the specified partition.
     * Returns null if no record is found.
     *
     * @param partitionId a partition Id
     * @return a target migration record or null
     */
    TargetRecord getTarget(PartitionId partitionId) {
        final MigrationRecord record = get(partitionId);
        return record instanceof TargetRecord ? (TargetRecord) record : null;
    }

    /**
     * Gets the source migration record associated with the specified partition.
     * Returns null if no record is found.
     *
     * @param partitionId a partition Id
     * @return a source migration record or null
     */
    SourceRecord getSource(PartitionId partitionId) {
        final MigrationRecord record = get(partitionId);
        return record instanceof SourceRecord ? (SourceRecord) record : null;
    }

    @Override
    public Iterator<MigrationRecord> iterator() {
        return records.values().iterator();
    }

    /**
     * Removes the record for the specified partition ID.
     *
     * @param partitionId a partition ID
     * @return the removed record or null
     */
    MigrationRecord remove(PartitionId partitionId) {
        return records.remove(partitionId);
    }

    /**
     * Returns an iteration over completed migration records.
     *
     * @return an iterator
     */
    Iterator<MigrationRecord> completed() {
        return new Iterator<MigrationRecord>() {

            final Iterator<MigrationRecord> itr = records.values().iterator();
            MigrationRecord r = null;

            @Override
            public boolean hasNext() {
                while (itr.hasNext()) {
                    r = itr.next();
                    if (r.isCompleted()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public MigrationRecord next() {
                if (r == null) {
                    throw new NoSuchElementException();
                }
                return r;
            }

            @Override
            public void remove() {
                if (r == null) {
                    throw new IllegalStateException();
                }
                itr.remove();
            }
        };
    }

    /**
     * Opens the partition migration database.
     */
    static Database openDb(Environment env,
                           DatabaseConfig dbConfig,
                           Transaction txn) {
        return env.openDatabase(txn, MIGRATION_DB_NAME, dbConfig);
    }

    /**
     * Fetches the PartitionMigrations object from the db for read-only use.
     * If none is in the db an empty object is returned. The returned object
     * should not be persisted.
     *
     * @param db the migration db
     * @return a PartitionMigrations object
     */
    static PartitionMigrations fetchRead(Database db) {

        final Transaction txn = db.getEnvironment().
                                  beginTransaction(null, NO_WAIT_CONFIG);
        try {
            PartitionMigrations migrations = get(db, txn,
                                                 LockMode.READ_UNCOMMITTED);
            if (migrations == null) {
                migrations = new PartitionMigrations();
            }
            migrations.version = READ_ONLY_VERSION;
            return migrations;
        } finally {

            /* We are just reading. */
            if (txn.isValid()) {
                txn.commit();
            } else {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Fetches the PartitionMigrations object from the db for write.
     *
     * @param db the migration db
     * @param txn the transaction to use for the get
     * @return a PartitionMigrations object
     */
    static PartitionMigrations fetchWrite(Database db, Transaction txn) {
        PartitionMigrations migrations = get(db, txn, LockMode.RMW);
        if (migrations != null) {
            return migrations;
        }
        /*
         * No object was found. Create an empty object and write it so that
         * the record is locked.
         */
        migrations = new PartitionMigrations();
        final OperationStatus status =
                db.putNoOverwrite(txn, migrationsKey,
                     new DatabaseEntry(SerializationUtil.getBytes(migrations)));
        if (status.equals(OperationStatus.SUCCESS)) {
            return migrations;
        }
        assert status.equals(OperationStatus.KEYEXIST);

        /* The record was written before us, redo the get */
        migrations = get(db, txn, LockMode.RMW);
        if (migrations == null) {
            throw new IllegalStateException("No migrations object found");
        }
        return migrations;
    }

    /**
     * Gets the PartitionMigrations object from the db. If none
     * is in the db null is returned.
     *
     * @param db the migration db
     * @param txn the transaction to use for the get
     * @param lockMode for the get
     * @return a PartitionMigrations object or null
     */
    static private PartitionMigrations get(Database db,
                                           Transaction txn,
                                           LockMode lockMode) {
        if (txn == null) {
            throw new IllegalStateException("transaction can not be null");
        }

        final DatabaseEntry value = new DatabaseEntry();

        db.get(txn, migrationsKey, value, lockMode);

        return SerializationUtil.getObject(value.getData(),
                                           PartitionMigrations.class);
    }

    /**
     * Persists this object to the db using the specified transaction.
     *
     * @param db the migration db
     * @param txn transaction to use for the write
     * @param bumpChangeNum if true the change number is incremented
     */
    void persist(Database db, Transaction txn, boolean bumpChangeNum) {
        if (version == READ_ONLY_VERSION) {
            throw new IllegalStateException("Attempt to persist read-only" +
                                            " copy");
        }
        if (bumpChangeNum) {
            changeNumber++;
        }
        db.put(txn, migrationsKey,
               new DatabaseEntry(SerializationUtil.getBytes(this)));
    }

    /**
     * Constructs and returns a new migration target record.
     */
    TargetRecord newTarget(PartitionId partitionId,
                           RepGroupId sourceRGId,
                           RepNodeId targetRNId) {
        return new TargetRecord(partitionId, sourceRGId, targetRNId,
                                nextRecordId++);
    }

    /**
     * Constructs and returns a new migration source record.
     */
    SourceRecord newSource(PartitionMigrationStatus status,
                           PartitionId partitionId,
                           RepGroupId sourceRGId,
                           RepNodeId targetRNId) {
        return new SourceRecord(status, partitionId, sourceRGId, targetRNId,
                                nextRecordId++);
    }

    private void readObject(ObjectInputStream ois)
            throws IOException, ClassNotFoundException {

	ois.defaultReadObject();

        if (version > CURRENT_SCHEMA_VERSION) {
            throw new IllegalStateException
                ("The Partition Migration Service is at " +
                 KVVersion.CURRENT_VERSION +
                 ", schema version " +  CURRENT_SCHEMA_VERSION +
                 " but the stored schema is at version " + version +
                 ". Please upgrade this node's NoSQL Database version.");
        }

        /* In R2 there is only one version in existence */
        if (version != CURRENT_SCHEMA_VERSION) {
            throw new IllegalStateException
                ("Unexpected migration store schema version, expected " +
                 CURRENT_SCHEMA_VERSION +
                 " but the stored schema is at version " + version + ".");
        }
    }

    @Override
    public String toString() {
        return "PartitionMigrations[" + changeNumber +
               ", " + topoSequenceNum + ", " + records.size() + "]";
    }

    /**
     * Print this object as a JSON string using the specified writer.
     */
    void print(FileWriter mdWriter) throws IOException {
        ObjectNode migrationRec = JsonUtils.createObjectNode();
        migrationRec.put("changeNum", changeNumber);
        migrationRec.put("topoSequenceNum", topoSequenceNum);
        migrationRec.put("nextRecordId", nextRecordId);
        migrationRec.put("version", version);

        if (!records.isEmpty()) {
            for (MigrationRecord record : records.values()) {
                if (record.getStatus() != null) {
                    String jsonFormattedStr = JsonUtils.prettyPrint(record);
                    ObjectNode jnodeMigrationRecord = JsonUtils
                                     .parseJsonObject(jsonFormattedStr);
                    migrationRec.put(record.getPartitionId().getPartitionName(),
                                     jnodeMigrationRecord);
                }
            }
            ObjectNode jnodeMigrationRecordMain = JsonUtils.createObjectNode();
            jnodeMigrationRecordMain.set("PartitionMigrationMetadata",
                                          migrationRec);
            mdWriter.write(JsonUtils.writeAsJson(jnodeMigrationRecordMain,
                                                 true));
            mdWriter.write(System.getProperty("line.separator"));
        } else {
            System.out.println("No active migration is going on. So partition"
                    + " migration data do not available at this time.");
        }
    }

    /*
     * Object to persist the target side of a partition migration.
     */
    static class TargetRecord extends MigrationRecord {
        private static final long serialVersionUID = 1L;

        /**
         * Constructor for a target record. Until the status is set getState()
         * will return PENDING.
         */
        private TargetRecord(PartitionId partitionId,
                             RepGroupId sourceRGId,
                             RepNodeId targetRNId,
                             long recordId) {
            super(null, partitionId, sourceRGId, targetRNId, recordId);
        }

        /**
         * Gets the state of the migration. If the status has not been set
         * PENDING is returned, otherwise getStatus().getState() is returned.
         *
         * @return the state of the migration
         */
        PartitionMigrationState getState() {
            return (status == null) ? PartitionMigrationState.PENDING :
                                      status.getState();
        }

        /**
         * Sets the status.
         */
        void setStatus(PartitionMigrationStatus status) {
            assert status.forTarget();
            this.status = status;
        }

        /**
         * Returns true if the state is PENDING.
         */
        boolean isPending() {
            return getState().equals(PartitionMigrationState.PENDING);
        }

        @Override
        boolean isCompleted() {
            return getState().equals(PartitionMigrationState.SUCCEEDED);
        }

        @Override
        public String toString() {
            return "TargetRecord[" + getPartitionId() + ", " +
                   getSourceRGId() + ", " + getTargetRGId() + ", " +
                   getState() + "]";
        }
    }

    /*
     * Object to persist the source side of a partition migration. Note that
     * this object is only stored when the source operation is complete.
     */
    static class SourceRecord extends MigrationRecord {
        private static final long serialVersionUID = 1L;

        /**
         * Constructor for a source record.
         */
        private SourceRecord(PartitionMigrationStatus status,
                             PartitionId partitionId,
                             RepGroupId sourceRGId,
                             RepNodeId targetRNId,
                             long recordId) {
            super(status, partitionId, sourceRGId, targetRNId, recordId);
            assert status.forSource();
        }

        @Override
        boolean isCompleted() {
            return true;
        }

        @Override
        public String toString() {
            return "SourceRecord[" + getPartitionId() + ", " +
                   getSourceRGId() + ", " + getTargetRNId() + "]";
        }
    }

    /**
     * Base object to persist partition migration information.
     */
    static abstract class MigrationRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        private final PartitionId partitionId;
        private final RepGroupId sourceRGId;

        /*
         * On the target, the RN is the node where the request was initially
         * received.
         */
        private final RepNodeId targetRNId;
        private final long recordId;

        /*
         * The status information for this migration.
         */
        protected PartitionMigrationStatus status = null;

        /**
         * Constructor.
         */
        protected MigrationRecord(PartitionMigrationStatus status,
                                  PartitionId partitionId,
                                  RepGroupId sourceRGId,
                                  RepNodeId targetRNId,
                                  long recordId) {
            this.status = status;
            this.partitionId = partitionId;
            this.sourceRGId = sourceRGId;
            this.targetRNId = targetRNId;
            this.recordId = recordId;
        }

        PartitionId getPartitionId() {
            return partitionId;
        }

        RepGroupId getSourceRGId() {
            return sourceRGId;
        }

        RepGroupId getTargetRGId() {
            return new RepGroupId(targetRNId.getGroupId());
        }

        RepNodeId getTargetRNId() {
            return targetRNId;
        }

        long getId() {
            assert recordId != 0;
            return recordId;
        }

        /**
         * Gets the status if set.
         *
         * @return the status or null
         */
        PartitionMigrationStatus getStatus() {
            return status;
        }

        /**
         * Returns true if this record represents a completed operation.
         */
        abstract boolean isCompleted();
    }
}
