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

package oracle.kv.impl.rep.migration.generation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;

/**
 * Object represents the manager that manages the underlying JE database for
 * partition generation table
 */
public class PartitionGenDBManager {

    /* partition generation database name */
    static final String PARTITION_GEN_MD_DB_NAME = "PartitionGenMD";

    /* read lock mode in get */
    private static final LockMode READ_LOCK_MODE = LockMode.DEFAULT;

    /* cursor config for scan */
    private static final CursorConfig CURSOR_CONFIG = CursorConfig.DEFAULT;

    /* txn config for get and put. */
    static final TransactionConfig TXN_CONFIG =
        new TransactionConfig().setDurability(
            new Durability(Durability.SyncPolicy.SYNC,
                           Durability.SyncPolicy.SYNC,
                           Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

    /* txn config for scan. */
    private static final TransactionConfig TXN_READ_COMMITTED_CONFIG =
        TXN_CONFIG.clone().setReadCommitted(true);

    /* prefix of all keys */
    private static final byte KEY_PREFIX = 'g';

    /* parent RN */
    private final RepNode repNode;

    /* private logger */
    private final Logger logger;

    PartitionGenDBManager(RepNode repNode, Logger logger) {

        this.repNode = repNode;
        this.logger = logger;
    }

    public static String getDBName() {
        return PARTITION_GEN_MD_DB_NAME;
    }

    /**
     * Opens the partition generation database.
     */
    public static Database openDb(Environment env,
                           DatabaseConfig dbConfig,
                           Transaction txn) {
        return env.openDatabase(txn, getDBName(), dbConfig);
    }

    /**
     * Retrieve list of partition generation object from partition generation
     * database.
     *
     */
    public static List<PartitionGeneration> getPartitionGenerationList(
            Database dbGeneration) {
        Cursor curGeneration = null;
        Transaction txnGeneration = null;
        try {
            final List<PartitionGeneration> listPartGen = new ArrayList<>();

            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();

            final TransactionConfig txnConfig = new TransactionConfig()
                    .setReadCommitted(true);

            txnGeneration = dbGeneration.getEnvironment().beginTransaction(null,
                    txnConfig);
            curGeneration = dbGeneration.openCursor(txnGeneration,
                    CursorConfig.DEFAULT);

            while (true) {
                final OperationStatus status = curGeneration.getNext(keyEntry,
                        dataEntry, null);
                if (status.equals(OperationStatus.SUCCESS)) {
                    listPartGen.add(PartitionGenDBManager
                            .readPartGenFromVal(dataEntry.getData()));
                } else {
                    break;
                }
            }

            return listPartGen;
        } finally {
            if (curGeneration != null) {
                curGeneration.close();
            }
            if (txnGeneration != null) {
                txnGeneration.commit();
            }
        }
    }

    /**
     * Begin a new transaction on generation database.
     *
     * @throws PartitionMDException if fail to begin a transaction
     */
    synchronized Transaction beginTransaction() throws PartitionMDException {
        final Database dbHandle = repNode.getMigrationManager()
                                         .getGenerationDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open on rn=" +
                       repNode.getRepNodeId());
        }
        try {
            return dbHandle.getEnvironment().beginTransaction(null, TXN_CONFIG);
        } catch (Exception exp) {
            logger.warning(lm("Fail to begin a transaction on " +
                             "db=" + PARTITION_GEN_MD_DB_NAME +
                              ", error=" + exp));
            throw new PartitionMDException(PARTITION_GEN_MD_DB_NAME,
                                           "Cannot begin a transaction on " +
                                           PARTITION_GEN_MD_DB_NAME,
                                           exp);
        }
    }

    /**
     * Puts a partition generation record to je database within a parent txn.
     *
     * @param txn        parent txn
     * @param pid        partition id
     * @param pg         partition generation record
     */
    synchronized void put(final Transaction txn,
                          final PartitionId pid,
                          final PartitionGeneration pg) {
        assert txn != null;

        final Database dbHandle = repNode.getMigrationManager()
                                         .getGenerationDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open on rn=" +
                                            repNode.getRepNodeId());
        }

        final DatabaseEntry key = buildKey(pid, pg.getGenNum());
        final DatabaseEntry val = buildValue(pg);

        dbHandle.put(txn, key, val);

        logger.fine(() -> lm("Done write db=" + PARTITION_GEN_MD_DB_NAME +
                             ", pid=" + pid + ", generation=" + pg));

    }

    /**
     * Test only for now but it would be useful to build tracing utility to
     * trace the migration history of a partition.
     *
     * Gets a partition generation from je database for a given partition
     * and the generation number. Returns the generation if successful, or null
     * if the record does not exist, or throw PartitionMDException if fail to
     * get from the je database.
     *
     * @param pid  partition id
     * @param pgn  partition generation number
     *
     * @return the record if successful, or null if the record does not exist
     *
     * @throws PartitionMDException if fail to get from je database
     */
    synchronized PartitionGeneration get(final PartitionId pid,
                                         final PartitionGenNum pgn)
        throws PartitionMDException {

        final Database dbHandle = repNode.getMigrationManager()
                                         .getGenerationDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open on rn=" +
                                            repNode.getRepNodeId());
        }

        final DatabaseEntry key = buildKey(pid, pgn);
        final DatabaseEntry val = new DatabaseEntry();
        /* made a db op, return if successful, throw exception otherwise */
        Transaction txn = null;
        OperationStatus status;
        try {
            txn = dbHandle.getEnvironment().beginTransaction(null, TXN_CONFIG);
            status = dbHandle.get(txn, key, val, READ_LOCK_MODE);
            txn.commit();
            txn = null;
            if (status.equals(OperationStatus.SUCCESS)) {
                return readPartGenFromVal(val.getData());
            }
            /* not found */
            return null;
        } catch (Exception exp) {
            logger.warning(lm("Fail to look up db=" + PARTITION_GEN_MD_DB_NAME +
                              ", pid=" + pid  + ", gen#=" + pgn +
                              ", error=" + exp));

            throw new PartitionMDException(PARTITION_GEN_MD_DB_NAME,
                                           "Fail to look up db=" +
                                           PARTITION_GEN_MD_DB_NAME +
                                           ", pid=" + pid  + ", gen#=" + pgn,
                                           null, exp);
        } finally {
            TxnUtil.abort(txn);
        }
    }

    /**
     * Check if generation database is empty.
     *
     * @return true if database is empty, false otherwise
     *
     * @throws PartitionMDException if fail to complete the scan
     */
    synchronized boolean isEmpty() throws PartitionMDException {

        return (scan(null /* filter */, true /* checkDbEmptyOnly */) == null);
    }

    /**
     * Scans the JE database with called implemented filter, used to return
     * generation records from database or check the emptiness of database.
     *
     * @param filter filter that is used to filter each record in database
     *
     * @param checkDbEmptyOnly whether to check database is empty only. If true,
     *        return an EMPTY_LIST immediately after the first record is read
     *        from the database.
     *
     * @return set of partition generation records read from database, if
     *         checkDbEmptyOnly is true return an EMPTY_LIST when database
     *         is not empty, return null if no record in database
     *
     * @throws PartitionMDException if fail to complete the scan
     */
    private synchronized List<PartitionGeneration> scan(
        final MetadataFilter filter, final boolean checkDbEmptyOnly)
        throws PartitionMDException {

        final Database dbHandle = repNode.getMigrationManager()
                                         .getGenerationDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open on rn=" +
                                            repNode.getRepNodeId());
        }
        List<PartitionGeneration> ret = null;
        Cursor cursor = null;
        Transaction txn = null;
        try {
            int nRecords = 0;
            txn = dbHandle.getEnvironment().beginTransaction(
                null, TXN_READ_COMMITTED_CONFIG);
            cursor = dbHandle.openCursor(txn, CURSOR_CONFIG);
            while (true) {
                final DatabaseEntry keyEntry = new DatabaseEntry();
                final DatabaseEntry dataEntry = new DatabaseEntry();
                final OperationStatus status = cursor.getNext(keyEntry,
                                                              dataEntry,
                                                              null);
                if (status.equals(OperationStatus.SUCCESS)) {
                    if (checkDbEmptyOnly) {
                        /*
                         * Database is not empty, return an empty list
                         * immediately after the first record is found.
                         */
                        return Collections.emptyList();
                    }
                    nRecords++;

                    /* if filter enabled and fail to pass */
                    if (filter != null &&
                        !filter.filter(keyEntry, dataEntry)) {
                        continue;
                    }
                    if (ret == null) {
                        ret = new ArrayList<>();
                    }
                    ret.add(readPartGenFromVal(dataEntry.getData()));
                } else {
                    break;
                }
            }
            cursor.close();
            cursor = null;
            txn.commit();
            txn = null;
            logger.info(lm("Finish scan db=" + PARTITION_GEN_MD_DB_NAME +
                           "records #scanned=" + nRecords +
                           ", #returned=" + ((ret == null) ? 0 : ret.size())));

            return ret;
        } catch (Exception exp) {
            logger.warning(lm("Fail to scan db= " + PARTITION_GEN_MD_DB_NAME +
                              ", error=" + exp));
            throw new PartitionMDException(PARTITION_GEN_MD_DB_NAME,
                                           "Cannot scan db=" +
                                           PARTITION_GEN_MD_DB_NAME,
                                           exp);
        } finally {
            if (cursor != null) {
                TxnUtil.close(cursor);
            }
            TxnUtil.abort(txn);
        }
    }

    /**
     * Scans the JE database with called implemented filter
     *
     * @param filter filter that is used to filter each record in database
     *
     * @return set of partition generation records, return null if no record
     *         in database
     *
     * @throws PartitionMDException if fail to complete the scan
     */
    synchronized List<PartitionGeneration> scan(final MetadataFilter filter)
        throws PartitionMDException {

        return scan(filter, false /* checkDbEmptyOnly */);
    }

    /**
     * Scans the JE database without any filter
     *
     * @return set of partition generation records, return null if no record
     *         in database
     *
     * @throws PartitionMDException if fail to complete the scan
     */
    synchronized List<PartitionGeneration> scan() throws PartitionMDException {
        return scan(null, false /* checkDbEmptyOnly */);
    }

    /**
     * Gets the currently last VLSN in VLSN index
     *
     * @return the currently last VLSN in VLSN index
     */
    long getLastVLSN() {
        final ReplicatedEnvironment repEnv = repNode.getEnv(0);
        final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
        if (repImpl != null) {
            return repImpl.getVLSNIndex().getRange().getLast();
        }

        throw new IllegalStateException("Invalid replicated env");
    }

    /**
     * Builds compound key for je database from partition id and partition
     * generation number
     *
     * @param pid partition id
     * @param pgn partition generation number
     *
     * @return compound key
     */
    static DatabaseEntry buildKey(PartitionId pid, PartitionGenNum pgn) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (final DataOutputStream dos = new DataOutputStream(bos)) {
            dos.write(KEY_PREFIX);
            SerializationUtil.writePackedInt(dos, pid.getPartitionId());
            SerializationUtil.writePackedInt(dos, pgn.getNumber());
            dos.flush();

            final DatabaseEntry keyEntry = new DatabaseEntry();
            keyEntry.setData(bos.toByteArray());
            return keyEntry;
        } catch (IOException ioe) {
            throw new IllegalStateException("Unexpected IOException=" + ioe,
                                            ioe);
        }
    }

    /**
     * Reads partition id from key byte array
     *
     * @param key key byte array
     *
     * @return partition key
     */
    public static PartitionId readPartIdFromKey(byte[] key) {
        final ByteArrayInputStream bis = new ByteArrayInputStream(key);
        try (final DataInputStream dis = new DataInputStream(bis)) {
            final byte prefix = dis.readByte();
            if (prefix != KEY_PREFIX) {
                throw new IllegalArgumentException("Invalid key with " +
                                                   "unrecognized prefix=" +
                                                   prefix +
                                                   ", expected=" + KEY_PREFIX);
            }

            final int id = SerializationUtil.readPackedInt(dis);
            return new PartitionId(id);
        } catch (IOException ioe ) {
            throw new IllegalStateException("Unexpected IOException=" + ioe,
                                            ioe);
        }
    }

    /**
     * Reads partition generation number from key byte array
     *
     * @param key key byte array
     *
     * @return partition generation number
     */
    static PartitionGenNum readGenNumFromKey(byte[] key) {
        final ByteArrayInputStream bis = new ByteArrayInputStream(key);
        try (final DataInputStream dis = new DataInputStream(bis)) {
            final byte prefix = dis.readByte();
            if (prefix != KEY_PREFIX) {
                throw new IllegalArgumentException("Invalid key with " +
                                                   "unrecognized prefix=" +
                                                   prefix +
                                                   ", expected=" + KEY_PREFIX);
            }

            SerializationUtil.readPackedInt(dis);
            final int gen = SerializationUtil.readPackedInt(dis);
            return new PartitionGenNum(gen);
        } catch (IOException ioe ) {
            throw new IllegalStateException("Unexpected IOException=" + ioe,
                                            ioe);
        }
    }

    /**
     * Builds value for je database from partition generation record
     *
     * @param pg partition generation
     *
     * @return value for je database
     */
    static DatabaseEntry buildValue(PartitionGeneration pg) {
        final DatabaseEntry valEntry = new DatabaseEntry();
        valEntry.setData(SerializationUtil.getBytes(pg));
        return valEntry;
    }

    /**
     * Builds partition generation for value get from je database
     *
     * @param data byte array data get from je database
     *
     * @return partition generation
     */
    public static PartitionGeneration readPartGenFromVal(byte[] data) {
        /* using Java deserialization defined in partition generation */
        return SerializationUtil.getObject(data, PartitionGeneration.class);
    }

    /**
     * Interface of filtering function applied in database scan
     */
    interface MetadataFilter {

        /**
         * Invokes filtering for each (key, value) read from the JE database,
         * returns true if the key value pair satisfies the condition.
         *
         * Parser APIs are provided to parse the partition id and partition
         * generation number from key, and partition generation from value.
         *
         * @see PartitionGenDBManager#readPartIdFromKey
         * @see PartitionGenDBManager#readGenNumFromKey
         * @see PartitionGenDBManager#readPartGenFromVal
         *
         * @return true if the key value pass the filter, false otherwise
         * blocked
         */
        boolean filter(DatabaseEntry key, DatabaseEntry val);
    }

    private String lm(String msg) {
        return "[PGDB-" + repNode.getRepNodeId() + "] " + msg;
    }

    /**
     * Test only
     * @return generation db handle
     */
    Database getDB() {
        return repNode.getMigrationManager().getGenerationDB();
    }
}
