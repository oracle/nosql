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

package oracle.kv.impl.pubsub;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.WriteOptions;

/**
 * Object represent the checkpoint table used in subscription.
 */
public class CheckpointTableManager {

    /**
     * rate limiting log period in ms
     */
    private static final int RL_LOG_PERIOD_MS = 5 * 60 * 1000;

    /**
     * rate limiting log max objects
     */
    private static final int RL_LOG_MAX_OBJ = 1024;

    /**
     * checkpoint table write timeout in ms
     */

    //TODO: hardcoded for now, may make it configurable in publisher config
    // if necessary
    final static int CKPT_TIMEOUT_MS = KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;

    /**
     * retry interval in ms
     */
    final static int RETRY_INTERVAL_MS = 3000;
    /**
     * maximum number of attempts
     */
    final static int MAX_NUM_ATTEMPTS = 20;

    /**
     * read checkpoint from master
     */
    private final static ReadOptions READ_OPT =
        new ReadOptions(Consistency.ABSOLUTE, 0, null);

    /**
     * shift of shard id to support elasticity
     */
    private final static int ELASTIC_SHARD_ID_SHIFT = -1000 * 1000;

    /**
     * The checkpoint table is a user table persisted in source kvstore which
     * records the checkpoint of each shard for a given subscription. Any
     * subscription need to be eligible to read and write the checkpoint
     * table associated with that subscription.
     * <p>
     * The checkpoint table schema example for subscription with checkpoint
     * table name my_ckpt
     * <p>
     * subscription |   shard id  |   vlsn   |  timestamp in ms
     * ------------------------------------------------------------------
     * my_ckpt   |       1     |   1024   |  232789789781789
     * my_ckpt   |       2     |   2048   |  258246582346578
     * my_ckpt   |       3     |   3076   |  234527348572834
     * ------------------------------------------------------------------
     * my_ckpt   |  -1000003   |   4096   |  234527821572901
     * ------------------------------------------------------------------
     * <p>
     * Note
     * 1) All rows of a checkpoint table are clustered on single shard with
     * the shard key. For example, all rows for subscription "s1" are all
     * clustered on a single shard computed from shard key "s1".
     * <p>
     * 2) The checkpoint table is a user table specified by user in
     * subscription configuration (@see NoSQLSubscriptionConfig).
     * <p>
     * 3) Updates to the checkpoint table cannot be subscribed.
     * <p>
     * 4) If shard id is negative, the row is created by an internal
     * component to support elastic operations with a shifted shard id.
     * <p>
     * If shard is positive, the row is part of an external checkpoint made
     * by user.
     * <p>
     * 5) shard key of the table is "subscription".
     */

    /* write option to put checkpoint row in kvstore */
    private final static WriteOptions CKPT_WRITE_OPT =
        new WriteOptions(Durability.COMMIT_SYNC,
                         CKPT_TIMEOUT_MS, /* max time to do a checkpoint */
                         TimeUnit.MILLISECONDS);

    private final static String CKPT_TABLE_SUBSCRIPTION_FIELD_NAME =
        "subscription";
    final static String CKPT_TABLE_SHARD_FIELD_NAME = "shard_id";
    final static String CKPT_TABLE_VLSN_FIELD_NAME = "vlsn";
    final static String CKPT_TABLE_TS_FIELD_NAME = "timestamp";

    /* list of all checkpoint fields */
    private final static List<String> LIST_OF_CKPT_FIELDS =
        Arrays.asList(CKPT_TABLE_SHARD_FIELD_NAME,
                      CKPT_TABLE_SUBSCRIPTION_FIELD_NAME,
                      CKPT_TABLE_TS_FIELD_NAME,
                      CKPT_TABLE_VLSN_FIELD_NAME);

    /* private logger */
    private final Logger logger;
    /* subscriber that uses this checkpoint table */
    private final NoSQLSubscriberId sid;
    /* a handle of source kvstore */
    private final KVStoreImpl kvstore;

    /**
     * cached checkpoint table instance, populated at initialization and
     * used in all subsequent checkpoints. The checkpoint table schema is
     * not changed in stream
     */
    private volatile Table checkpointTable;

    /**
     * parent PU
     */
    private final PublishingUnit parent;

    /**
     * rate limiting logger
     */
    private final RateLimitingLogger<String> rlLogger;

    /**
     * A map from the subscriber id index to the checkpoint table name. This
     * map includes both local (its own subscriber) and non-local checkpoint
     * table name from other subscribers. For single subscriber, there is no
     * non-local checkpoint table name in the map. The map is populated in
     * constructor and read-only after that. The local checkpoint table is used
     * when the stream reads and writes its own checkpoint. The non-local
     * checkpoint tables are used to process records from store elastic
     * operations.
     */
    private final Map<Integer, String> ckptTableNameMap;


    public CheckpointTableManager(PublishingUnit parent, Logger logger) {
        this.parent = parent;
        this.logger = logger;
        kvstore = parent.getKVStore();
        sid = parent.getSubscriberId();
        checkpointTable = null;
        rlLogger = new RateLimitingLogger<>(
            RL_LOG_PERIOD_MS, RL_LOG_MAX_OBJ, logger);
        ckptTableNameMap = new HashMap<>();
        /* populate checkpoint map */
        ckptTableNameMap.putAll(parent.getCkptTableNameMap());
        /* populate its own checkpoint table */
        ckptTableNameMap.put(sid.getIndex(), parent.getCkptTableName());
        logger.info(lm("Checkpoint table map is set to=" + ckptTableNameMap));
    }

    /**
     * Unit test only
     */
    public CheckpointTableManager(KVStoreImpl kvstore,
                                  NoSQLSubscriberId sid,
                                  String ckptTableName,
                                  Logger logger) {
        this.kvstore = kvstore;
        this.sid = sid;
        this.logger = logger;
        parent = null;
        checkpointTable = null;
        rlLogger = new RateLimitingLogger<>(
            RL_LOG_PERIOD_MS, RL_LOG_MAX_OBJ, logger);
        ckptTableNameMap = new HashMap<>();
        /* populate its own checkpoint table */
        ckptTableNameMap.put(sid.getIndex(), ckptTableName);
        logger.info(lm("Checkpoint table map is set to=" + ckptTableNameMap));
    }

    /**
     * Creates checkpoint table. Throw an exception if the table exists or it
     * fails to create the table.
     *
     * @param kvs           handle to kvstore
     * @param ckptTableName name of checkpoint table
     * @throws IllegalArgumentException checkpoint table already exists at
     *                                  store or fail to create the checkpoint
     * @throws UnauthorizedException    user is not eligible to create the
     *                                  checkpoint table at kvstore
     */
    public static void createCkptTable(KVStore kvs, String ckptTableName)
        throws IllegalArgumentException, UnauthorizedException {
        createCkptTable(kvs, ckptTableName, false);
    }

    /**
     * Returns the complete checkpoint table as a string for logging
     *
     * @param kvs    kvstore handle
     * @param tbName name of the checkpoint table
     * @return a string built from checkpoint table
     */
    public static String ckptTblToString(KVStore kvs, String tbName) {
        final TableAPI tapi = kvs.getTableAPI();
        final Table table = tapi.getTable(tbName);
        if (table == null) {
            return "Checkpoint table not found, table name=" + tbName;
        }
        final List<Row> rows = getRowsFromCkptTable(kvs, table);
        final StringBuilder str = new StringBuilder(
            "subscription, shard, vlsn, timestamp\n");
        for (Row r : rows) {
            str.append(r.get(CKPT_TABLE_SUBSCRIPTION_FIELD_NAME).asString()
                        .get())
               .append(",")
               .append(r.get(CKPT_TABLE_SHARD_FIELD_NAME).asInteger().get())
               .append(", ")
               .append(r.get(CKPT_TABLE_VLSN_FIELD_NAME).asLong().get())
               .append(", ")
               .append(r.get(CKPT_TABLE_TS_FIELD_NAME).asLong().get())
               .append("\n");
        }
        return str.toString();
    }

    /**
     * Fetches a checkpoint made internally during elasticity and build a stream
     * position from checkpoint table
     *
     * @param shards shards of checkpoint
     * @return the last persisted checkpoint for given replication shards, or
     * null if no such checkpoint exists in kvstore
     */
    public StreamPosition fetchElasticCkpt(Set<RepGroupId> shards) {
        return fetchCkptHelper(shards, getCkptTableName(), true);
    }

    /**
     * Fetches a checkpoint made internally during elasticity from given
     * checkpoint table and build a stream position from checkpoint table
     *
     * @param shards shards of checkpoint
     * @param ckptTb checkpoint table name
     * @return the last persisted checkpoint for given replication shards, or
     * null if no such checkpoint exists in kvstore
     */
    StreamPosition fetchElasticCkpt(Set<RepGroupId> shards, String ckptTb) {
        return fetchCkptHelper(shards, ckptTb, true);
    }


    /**
     * Fetches a checkpoint made by user and build a stream position from
     * checkpoint table
     *
     * @param shards shards of checkpoint
     * @return the last persisted checkpoint for given replication shards, or
     * null if no such checkpoint exists in kvstore
     */
    public StreamPosition fetchCheckpoint(Set<RepGroupId> shards) {
        return fetchCkptHelper(shards, getCkptTableName(), false);
    }

    /**
     * Creates checkpoint table with retry on FaultException
     */
    void createCkptTable(boolean multiSubs) {
        /*
         * When admin is offline, FaultException will be thrown when creating
         * checkpoint table, therefore we need retry on FE
         */
        int attempts = 0;
        final String ckptTableName = getCkptTableName();
        while (!isShutdown()) {
            try {
                attempts++;
                createCkptTable(kvstore, ckptTableName, multiSubs);
                final int attemptsFinal = attempts;
                logger.info(lm("Checkpoint table=" + ckptTableName +
                               " created after attempts=" +
                               attemptsFinal));
                break;
            } catch (FaultException fe) {
                if (attempts == MAX_NUM_ATTEMPTS) {
                    final String err = "Cannot create checkpoint table=" +
                                       ckptTableName + " after max attempts=" +
                                       MAX_NUM_ATTEMPTS +
                                       ", multi-subs=" + multiSubs;
                    logger.warning(lm(err));
                    /* fail stream */
                    throw new SubscriptionFailureException(sid, err, fe);
                }

                /* sleep and retry */
                logger.info(lm("Cannot create checkpoint table=" +
                               ckptTableName + ", will retry after sleeping" +
                               "(ms)=" + RETRY_INTERVAL_MS +
                               ", attempts=" + attempts +
                               ", max attempts=" + MAX_NUM_ATTEMPTS +
                               ", multi-subs=" + multiSubs +
                               ", error=" + fe));
                try {
                    synchronized (this) {
                        wait(RETRY_INTERVAL_MS);
                    }
                } catch (InterruptedException ie) {
                    throw new SubscriptionFailureException(
                        sid, "Interrupted in creating checkpoint table=" +
                             checkpointTable, ie);
                }
            } catch (RuntimeException re) {
                final String err = "Cannot create checkpoint table=" +
                                   ckptTableName;
                logger.warning(lm(err));
                throw new SubscriptionFailureException(sid, err, re);
            }
        }
        if (isShutdown()) {
            logger.info(lm("Cannot create checkpoint table=" + ckptTableName +
                           " in shutdown"));
            return;
        }
        /*
         * populate checkpoint table instance, it must exist because it
         * is just created
         */
        getCkptTableWithRetry(Long.MAX_VALUE/* try till find */);
        logger.fine(() -> lm("Successfully get checkpoint table=" +
                             ckptTableName + " instance"));
    }

    /**
     * Checks if checkpoint table exists
     *
     * @return true if checkpoint exists for the given subscriber
     */
    boolean isCkptTableExists() {
        if (checkpointTable != null) {
            /* table instance already available */
            return true;
        }
        /*
         * table md is not reliable, but because we will do "create table if
         * not exists" anyway if checkpoint table is not found, we only need to
         * try once.
         */
        checkpointTable = getCkptTableWithRetry(1);
        return checkpointTable != null;
    }

    /**
     * Gets the table instance with retry to tolerate the non-deterministic
     * table md issue
     *
     * @param max # of attempts.
     * @return table instance
     */
    private Table getCkptTableWithRetry(long max) {
        if (checkpointTable != null) {
            /* already available */
            return checkpointTable;
        }

        int attempt = 0;
        final String ckptTableName = getCkptTableName();
        while (!isShutdown()) {
            attempt++;
            checkpointTable = kvstore.getTableAPI().getTable(ckptTableName);
            if (checkpointTable != null) {
                return checkpointTable;
            }
            if (attempt == max) {
                break;
            }
            rlLogger.log(ckptTableName, Level.INFO,
                         lm("Checkpoint table=" + ckptTableName +
                            " not found in attempts=" + attempt +
                            ", max attempts=" + max +
                            ", will retry after ms=" + RETRY_INTERVAL_MS));
            try {
                synchronized (this) {
                    wait(RETRY_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                final String err = "Interrupted when fetching checkpoint " +
                                   "table=-" + ckptTableName;
                throw new SubscriptionFailureException(sid, err);
            }
        }
        /* the checkpoint may not exist for new streams */
        logger.info(lm("Cannot find checkpoint table=" + ckptTableName +
                       " in attempts=" + attempt +
                       ", max attempts=" + max +
                       ", shutdown=" + isShutdown()));
        return null;
    }

    /**
     * Update the checkpoint for particular shard
     *
     * @param storeName store name
     * @param storeId   store id
     * @param gid       rep group od
     * @param vlsn      checkpoint vlsn
     * @return checkpoint made
     * @throws CheckpointFailureException if fail to checkpoint
     */
    StreamPosition updateShardCkpt(String storeName, long storeId,
                                   RepGroupId gid, long vlsn)
        throws CheckpointFailureException {
        return updateShardCkpt(storeName, storeId, gid, vlsn, false);
    }

    /**
     * Update the checkpoint for particular shard. The checkpoint is to
     * support elastic operations only and does not change the checkpoint
     * made by user.
     *
     * @param storeName store name
     * @param storeId   store id
     * @param gid       rep group od
     * @param vlsn      checkpoint vlsn
     * @return checkpoint made
     * @throws CheckpointFailureException if fail to checkpoint
     */
    StreamPosition updateElasticShardCkpt(String storeName, long storeId,
                                          RepGroupId gid, long vlsn)
        throws CheckpointFailureException {
        return updateShardCkpt(storeName, storeId, gid, vlsn,
                               true/* ckpt for elastic operations*/);
    }

    /**
     * Updates the checkpoint in kvstore with given stream position. If the
     * checkpoint cannot be updated or dropped, CFE is raised to caller
     *
     * @param pos stream position to checkpoint
     * @throws CheckpointFailureException if fail to update the checkpoint
     *                                    table in single transaction
     */
    void updateCkptTableInTxn(StreamPosition pos)
        throws CheckpointFailureException {
        updateCkptTableInTxn(pos, false/* user checkpoint */);
    }

    /**
     * Gets own checkpoint table name
     *
     * @return checkpoint table name
     */
    public String getCkptTableName() {
        return ckptTableNameMap.get(sid.getIndex());
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[CkptMan-" + getCkptTableName() + "-" + sid + "] " + msg;
    }

    /*
     * Constructs checkpoint rows from stream position
     */
    private Set<Row> createCkptRows(StreamPosition streamPos, boolean elastic) {

        final Set<Row> rows = new HashSet<>();

        final Table table = getCkptTableWithRetry(Long.MAX_VALUE);
        if (table == null) {
            logger.warning(lm("Checkpoint table=" + getCkptTableName() +
                              " not found"));
            return rows;
        }
        final Collection<StreamPosition.ShardPosition> allPos =
            streamPos.getAllShardPos();
        for (StreamPosition.ShardPosition pos : allPos) {
            final int shardId = pos.getRepGroupId();
            final long vlsn = pos.getVLSN();
            final Row row = table.createRow();
            row.put(CKPT_TABLE_SUBSCRIPTION_FIELD_NAME,
                    table.getFullNamespaceName());
            if (elastic) {
                row.put(CKPT_TABLE_SHARD_FIELD_NAME,
                        shiftShardId(shardId));
            } else {
                row.put(CKPT_TABLE_SHARD_FIELD_NAME, shardId);
            }
            row.put(CKPT_TABLE_VLSN_FIELD_NAME, vlsn);
            row.put(CKPT_TABLE_TS_FIELD_NAME, System.currentTimeMillis());
            rows.add(row);
        }

        return rows;
    }

    private static StreamPosition getPosFromCkptTable(KVStoreImpl kvs,
                                                      Set<RepGroupId> shards,
                                                      Table table,
                                                      boolean elastic)
        throws UnauthorizedException, StoreIteratorException {

        final String storeName = kvs.getTopology().getKVStoreName();
        final long storeId = kvs.getTopology().getId();
        final StreamPosition position = new StreamPosition(storeName, storeId);
        /* specify the shard key */
        final List<Row> rows = getRowsFromCkptTable(kvs, table);
        rows.stream()
            .filter(!elastic ?
                        /* only get regular ckpt rows */
                        CheckpointTableManager::regularCkptRow :
                        /* only get elastic operation ckpt rows */
                        r -> !regularCkptRow(r))
            /* only keep rows from given shards */
            .filter(row -> shards.contains(getRepGroupIdFromRow(row, elastic)))
            /* set shard position for each row */
            .forEach(row -> position.setShardPosition(
                getRepGroupIdFromRow(row, elastic).getGroupId(),
                getVLSNFromRow(row)));
        return position;
    }

    /**
     * Gets the all rows from a checkpoint table
     *
     * @param kvs   kvstore handle
     * @param table checkpoint table
     * @return list of rows in the checkpoint table
     */
    public static List<Row> getRowsFromCkptTable(KVStore kvs, Table table) {
        final PrimaryKey pkey = table.createPrimaryKey();
        pkey.put(CKPT_TABLE_SUBSCRIPTION_FIELD_NAME,
                 table.getFullNamespaceName());
        return kvs.getTableAPI().multiGet(pkey, null, READ_OPT);
    }

    /**
     * Returns true if the row is from a regular ckpt, false otherwise
     *
     * @param row given row
     * @return true if the row is from a regular ckpt, false otherwise.
     */
    private static boolean regularCkptRow(Row row) {
        return row.get(CKPT_TABLE_SHARD_FIELD_NAME).asInteger().get() >= 0;
    }

    /*
     * Returns shard id from given row
     *
     * @param row      given row
     * @param elastic  true if the row is created by elastic operation, false
     *                 if it is created by user
     * @return shard id from given row
     */
    public static RepGroupId getRepGroupIdFromRow(Row row, boolean elastic) {
        final int gid = row.get(CKPT_TABLE_SHARD_FIELD_NAME).asInteger().get();
        if (!elastic) {
            if (gid < 0) {
                throw new IllegalArgumentException("Row with negative " +
                                                   " shard id " + gid +
                                                   "cannot created by user");
            }
            return new RepGroupId(gid);
        }

        /* row should be from elastic operations */
        if (gid >= 0) {
            throw new IllegalArgumentException("Row with non-negative " +
                                               " shard id " + gid +
                                               "cannot created by elastic " +
                                               "operation");
        }
        /* negative shard id, map it back to regular shard id*/
        return new RepGroupId(reversedShiftShardId(gid));
    }

    /**
     * Returns true if the row from checkpoint table is from an elastic
     * operations, false otherwise
     *
     * @param row a row from checkpoint table
     * @return true if the row from checkpoint table is from an elastic
     * operation, or false otherwise
     */
    public static boolean isRowFromElasticOps(Row row) {
        final FieldValue fv = row.get(CKPT_TABLE_SHARD_FIELD_NAME);
        if (fv == null) {
            throw new IllegalArgumentException(
                "Not from checkpoint table, no field for col=" +
                CKPT_TABLE_SHARD_FIELD_NAME);
        }
        return fv.asInteger().get() < 0;
    }

    public static long getVLSNFromRow(Row row) {
        return row.get(CKPT_TABLE_VLSN_FIELD_NAME).asLong().get();
    }

    public static long getTimestampFromRow(Row row) {
        return row.get(CKPT_TABLE_TS_FIELD_NAME).asLong().get();
    }

    private static String getCreateCkptTableDDL(String tableName,
                                                boolean multiSubs) {

        final String ifNotExists = multiSubs ? " IF NOT EXISTS " : " ";
        return
            "CREATE TABLE" + ifNotExists + tableName +
            " (" +
            CKPT_TABLE_SUBSCRIPTION_FIELD_NAME + " STRING, " +
            CKPT_TABLE_SHARD_FIELD_NAME + " INTEGER, " +
            CKPT_TABLE_VLSN_FIELD_NAME + " LONG, " +
            CKPT_TABLE_TS_FIELD_NAME + " LONG, " +
            "PRIMARY KEY " +
            "(" +
            "SHARD(" + CKPT_TABLE_SUBSCRIPTION_FIELD_NAME + "), " +
            CKPT_TABLE_SHARD_FIELD_NAME + ")" +
            ")";
    }

    /**
     * Creates checkpoint table. Throw an exception if the table exists.
     * <p>
     * @param kvs             handle to kvstore
     * @param ckptTableName   name of checkpoint table
     * @param multiSubs       true if multiple subscribers
     * @throws IllegalArgumentException checkpoint table already exists at
     * store or fail to create the checkpoint
     * @throws UnauthorizedException user is not eligible to create the
     * checkpoint table at kvstore
     */
    public static void createCkptTable(KVStore kvs,
                                       String ckptTableName,
                                       boolean multiSubs)
        throws IllegalArgumentException, UnauthorizedException {

        final Table t = kvs.getTableAPI().getTable(ckptTableName);

        if (t != null) {
            /*
             * If single subscriber, we do not expect a checkpoint table, but
             * if multi-subscribers, other subscriber may just create it so it
             * is Ok.
             */
            if (multiSubs) {
                return;
            }
            /* single subscriber */
            throw new IllegalArgumentException("Existing checkpoint table " +
                                               ckptTableName);
        }

        /*
         * Exception UnauthorizedException will be thrown if not eligible to
         * create the checkpoint table
         */
        final StatementResult result =
            kvs.executeSync(getCreateCkptTableDDL(ckptTableName, multiSubs));
        if (!result.isSuccessful()) {
            throw new IllegalArgumentException(result.getErrorMessage());
        }
    }

    /**
     * Updates the checkpoint
     *
     * @param storeName store name
     * @param storeId   store id
     * @param gid       rep group od
     * @param vlsn      checkpoint vlsn
     * @param elastic   true if the checkpoint is for elastic operations
     * @return checkpoint made
     * @throws CheckpointFailureException if unable to update the checkpoint
     */
    private StreamPosition updateShardCkpt(String storeName, long storeId,
                                           RepGroupId gid, long vlsn,
                                           boolean elastic)
        throws CheckpointFailureException {

        final StreamPosition ckpt = new StreamPosition(storeName, storeId);
        ckpt.setShardPosition(gid.getGroupId(), vlsn);

        /* update the single row in ckpt table for given shard */
        updateCkptTableInTxn(ckpt, elastic);
        return ckpt;
    }

    /**
     * Updates the checkpoint in kvstore with given stream position. If the
     * checkpoint cannot be updated or dropped, CFE is raised to caller
     *
     * @param pos      stream position to checkpoint
     * @param internal true if the checkpoint is internal
     * @throws CheckpointFailureException if fail to update the checkpoint
     *                                    table in single transaction
     */
    private void updateCkptTableInTxn(StreamPosition pos, boolean internal)
        throws CheckpointFailureException {

        final String ckptTableName = getCkptTableName();
        try {

            final TableAPI tableAPI = kvstore.getTableAPI();
            /*
             * since start from a position, need ensure that ALL rows
             * are successfully written into the update table. If not,
             * exception is raised to caller.
             */
            final Set<Row> ckptRows = createCkptRows(pos, internal);

            /* write rows in transaction */
            final List<TableOperation> ops = new ArrayList<>();
            final TableOperationFactory f = tableAPI.getTableOperationFactory();
            for (Row row : ckptRows) {
                ops.add(f.createPut(row, null, true));
            }
            tableAPI.execute(ops, CKPT_WRITE_OPT);

            /* since it is unconditional put, every op must be successful */
            logger.fine(() -> lm("Checkpoint table updated successfully " +
                                 "to position=" + pos +
                                 ", full checkpoint=\n" +
                                 ckptTblToString(kvstore, ckptTableName)));
        } catch (UnauthorizedException ue) {
            final String err = "Subscriber=" + sid + " is not authorized to " +
                               "write table=" + ckptTableName;
            throw new CheckpointFailureException(sid, ckptTableName, err, ue);
        } catch (FaultException fe) {
            /*
             * With FaultException, there is no guarantee whether operation
             * completed successfully
             */
            final String err = "Unable to ensure the " +
                               "checkpoint table is updated" +
                               " successfully, position=" + pos;
            logger.warning(lm(err));
            throw new CheckpointFailureException(sid, ckptTableName,
                                                 "Unable to ensure the " +
                                                 "checkpoint table is updated" +
                                                 " successfully.", fe);
        } catch (TableOpExecutionException toee) {
            /* a sure failure */
            throw new CheckpointFailureException(sid, ckptTableName,
                                                 "Fail to persist the " +
                                                 "checkpoint table.", toee);
        }
    }

    /**
     * Fetches checkpoint for given shards
     *
     * @param shards    shard of checkpoint
     * @param ckptTable checkpoint table name
     * @param elastic   true if checkpoint for elastic operations, false if
     *                  user checkpoint
     * @return checkpoint for given shards
     * @throws SubscriptionFailureException if fail to fetch the checkpoint
     */
    private StreamPosition fetchCkptHelper(Set<RepGroupId> shards,
                                           String ckptTable,
                                           boolean elastic)
        throws SubscriptionFailureException {

        final TableAPI tableAPI = kvstore.getTableAPI();
        final boolean myCkpt = getCkptTableName().equals(ckptTable);
        Table table;
        int attempt = 0;
        /* Check for the table first */
        while (true) {
            attempt++;
            table = tableAPI.getTable(ckptTable);
            if (table != null) {
                final int attemptFinal = attempt;
                logger.fine(() -> "Ensure table=" + ckptTable +
                                  "(my ckpt=" + myCkpt + ")" +
                                  " exists after #attempts=" + attemptFinal);
                break;
            }

            if (attempt < MAX_NUM_ATTEMPTS) {
                /* Table might not be ready, wait and retry */
                try {
                    synchronized (this) {
                        wait(RETRY_INTERVAL_MS);
                    }
                } catch (InterruptedException e) {
                    final String err = "Interrupted when waiting for metadata" +
                                       " of table=" + ckptTable +
                                       "(my ckpt=" + myCkpt + ")" +
                                       " to be available in table api";
                    throw new SubscriptionFailureException(sid, err);
                }
            } else {
                /* no checkpoint table exists for the given subscriber */
                final String err = "After #attempts=" + attempt +
                                   " table=" + ckptTable +
                                   "(my ckpt=" + myCkpt + ")" +
                                   " still not found at store, terminate " +
                                   "subscription" +
                                   kvstore.getTopology().getKVStoreName();
                logger.warning(lm(err));
                throw new SubscriptionFailureException(sid, err);
            }
        }

        StreamPosition position;
        attempt = 0;
        while (true) {
            try {
                attempt++;
                position = getPosFromCkptTable(kvstore, shards, table, elastic);
                final String pos = position.toString();
                final int attemptFinal = attempt;
                logger.fine(() -> lm("After #attempts=" + attemptFinal +
                                     "subscription=" + sid + " read " +
                                     "checkpoint position=" + pos +
                                     " from table=" + ckptTable +
                                     "(my ckpt=" + myCkpt + ")"));
                break;
            } catch (StoreIteratorException sie) {
                if (attempt < MAX_NUM_ATTEMPTS) {
                    /* Table might not be ready, wait and retry */
                    try {
                        synchronized (this) {
                            wait(RETRY_INTERVAL_MS);
                        }
                    } catch (InterruptedException e) {
                        final String err = "Interrupted when waiting for " +
                                           "table=" + ckptTable +
                                           "(my ckpt=" + myCkpt + ")" +
                                           " to be ready";
                        throw new SubscriptionFailureException(sid, err);
                    }
                } else {
                    final String err = "After #attempts=" + attempt + ", " +
                                       "subscription=" + sid +
                                       "still cannot read checkpoint table=" +
                                       ckptTable + "(my ckpt=" + myCkpt + ")" +
                                       " from store=" +
                                       kvstore.getTopology().getKVStoreName() +
                                       ", terminate subscription.";
                    logger.warning(lm(err));
                    throw new SubscriptionFailureException(sid, err, sie);
                }
            } catch (UnauthorizedException ue) {
                final String err = "Subscription=" + sid +
                                   " is unauthorized to read from checkpoint " +
                                   "table=" + ckptTable +
                                   "(my ckpt=" + myCkpt + ")";
                logger.warning(lm(err));
                throw new SubscriptionFailureException(sid, err, ue);
            } catch (MetadataNotFoundException mnfe) {
                final int attemptFinal = attempt;
                logger.fine(() -> lm("Unable to read checkpoint table=" +
                                     ckptTable + "(my ckpt=" + myCkpt + ")" +
                                     " for subscriberId=" + sid +
                                     ", # attempts=" + attemptFinal +
                                     ", max # attempts=" + MAX_NUM_ATTEMPTS +
                                     ", error=" + mnfe));
                if (attempt < MAX_NUM_ATTEMPTS) {
                    /* Table might not be ready, wait and retry */
                    try {
                        synchronized (this) {
                            wait(RETRY_INTERVAL_MS);
                        }
                    } catch (InterruptedException e) {
                        final String err = "Interrupted when waiting for " +
                                           "table=" + ckptTable +
                                           "(my ckpt=" + myCkpt + ")" +
                                           " to be ready";
                        throw new SubscriptionFailureException(sid, err);
                    }
                } else {
                    final String err = "After #attempts=" + attempt +
                                       ", subscription id=" + sid +
                                       "still cannot read checkpoint table=" +
                                       ckptTable + "(my ckpt=" + myCkpt + ")" +
                                       " from store=" +
                                       kvstore.getTopology().getKVStoreName() +
                                       ", terminate subscription.";
                    logger.warning(lm(err));
                    throw new SubscriptionFailureException(sid, err, mnfe);
                }
            } catch (Exception exp) {
                final String err = "Cannot read checkpoint table=" +
                                   ckptTable + "(my ckpt=" + myCkpt + ")" +
                                   " for id=" + sid;
                logger.warning(lm(err + ", error=" + exp));
                throw new SubscriptionFailureException(
                    sid, err + ", error=" + exceptionString(exp), exp);
            }
        }

        /* check if we miss any shards */
        for (RepGroupId gid : shards) {
            if (position.getShardPosition(gid.getGroupId()) == null) {
                /* missing shard from checkpoint table, fix it */
                position.setShardPosition(gid.getGroupId(), NULL_VLSN);
            }
        }

        return position;
    }

    /**
     * Returns checkpoint VLSN of a checkpoint table at given shard, this is
     * only used to fetch internal checkpoint.
     */
    public static long getCkptVLSN(KVStore kvs, Table table, int gid) {
        if (!isCkptTable(table)) {
            throw new IllegalArgumentException("not a checkpoint table=" +
                                               table.getFullName());
        }
        final Set<RepGroupId> shards = new HashSet<>();
        shards.add(new RepGroupId(gid));
        final StreamPosition sp =
            getPosFromCkptTable((KVStoreImpl) kvs, shards, table,
                                true/* internal checkpoint only*/);
        if (sp.getShardPosition(gid) == null) {
            return NULL_VLSN;
        }
        return sp.getShardPosition(gid).getVLSN();
    }

    /**
     * Returns true if the table is a checkpoint table
     */
    public static boolean isCkptTable(Table table) {
        /*
         * TODO: The TableAPI does not support internal id or hidden prefix
         * of table name that can be used to recognize a class of tables.
         * Therefore, unless we have better way to determine if a given table
         * is a checkpoint table, we check by matching the table schema.
         */
        return table.getFields().containsAll(LIST_OF_CKPT_FIELDS);
    }

    /**
     * Shifts a regular shard id to an internal id
     *
     * @param regularId regular shard id
     * @return internal id
     */
    private static int shiftShardId(int regularId) {
        if (regularId < 0) {
            throw new IllegalArgumentException("Regular shard id cannot be " +
                                               "negative");
        }
        /* negate id and plus a shift */
        return Math.negateExact(regularId) + ELASTIC_SHARD_ID_SHIFT;
    }

    /**
     * Shifts an internal id to a regular shard id
     *
     * @param internalId internal id
     * @return regular shard id
     */
    private static int reversedShiftShardId(int internalId) {
        if (internalId >= 0) {
            throw new IllegalArgumentException("Internal id cannot be " +
                                               "non-negative");
        }

        /* minus a shift and take a negative */
        return Math.negateExact(internalId - ELASTIC_SHARD_ID_SHIFT);
    }

    private boolean isShutdown() {
        if (parent == null) {
            /* unit test only */
            return false;
        }
        return parent.isClosed();
    }

    String getCheckpointTableName(int index) {
        return ckptTableNameMap.get(index);
    }
}