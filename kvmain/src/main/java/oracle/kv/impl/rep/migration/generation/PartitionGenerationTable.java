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

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.impl.pubsub.NoSQLStreamFeederFilter;
import oracle.kv.impl.rep.PartitionManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.je.Database;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.FeederReplicaSyncup;
import com.sleepycat.je.utilint.VLSN;

/**
 * Object represents a conceptual table that records migration history of
 * partitions hosted by the RN. The table is shard owned, e.g., each shard
 * maintains its own partition generation table. The table is initialized
 * when the first partition migration is performed on master only, from an
 * internal replicated JE database which replicates from master to to replicas.
 * When the initialization completes, a partition generation record with
 * PartitionId.NULL_ID is persisted as INIT_DONE marker. If no INIT_DONE marker
 * found in database, master will try to re-initialize this table.
 *
 * The table is only used and updated at master RN. In particular, open/close
 * of the table is managed in two places:
 * 1. When a master RN starts up (shutdown), the table is opened (closed)
 * when the partition db is opened (closed) in PartitionManager;
 * 2. In RN state transition, open/close of the table is managed in the state
 * tracker of {@link oracle.kv.impl.rep.migration.MigrationManager}. The
 * state tracker tracks the state transition of RN and opens the table when
 * RN becomes master and closes it when the RN is not longer the master.
 */
public class PartitionGenerationTable {

    /**
     * Static generation record put in the generation database only,
     * used to check if first initialization is done after scan database.
     * This entry uses PartitionId.NULL_ID as the id, should only be scaned
     * from database to mark that the table contains all the partitions
     * that were owned by this shard before any partition migration occurred.
     * This entry is ignored otherwise. The node that has not been upgraded
     * would load this entry to the in-memory table if it becomes to a master
     * during the upgrade, but it's harmless and won't match any lookup since
     * it wasn't a valid partition id.
     */
    private static final PartitionGeneration INIT_DONE_MARKER =
        new PartitionGeneration(PartitionGeneration.getInitDoneMarkerId());

    /** private logger */
    private final Logger logger;

    /** manager dealing with underlying je database */
    private final PartitionGenDBManager dbManager;

    /**
     * Generation table indexed by partition id or null if not initialized.
     * For each partition, a sorted map on partition generation number is used
     * to represent the history for that partition. Synchronize on the
     * PartitionGenerationTable object when modifications and to iterate over
     * a consistent version of the full structure.
     */
    private volatile ConcurrentMap<PartitionId,
        SortedMap<PartitionGenNum, PartitionGeneration>> genTable;

    /** set to true after the table is initialized */
    private volatile boolean ready;

    /** parent rep node */
    private final RepNode repNode;

    /* Test hook may be invoked before initDone is persisted */
    private volatile TestHook<Integer> beforeInitDoneHook;

    public PartitionGenerationTable(RepNode repNode, Logger logger) {

        this.repNode = repNode;
        this.logger = logger;

        this.dbManager = new PartitionGenDBManager(repNode, logger);
        ready = false;
    }

    /**
     * Returns true if the table is initialized and ready to use, false
     * otherwise.
     *
     * @return true if the table is initialized and ready to use, false
     * otherwise.
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * Returns true if the partition generation table has been initialized
     * before. It scans the generation database to see if any generation record
     * exists. Return true if found any records, false otherwise.
     *
     * @return true if the partition generation table has been initialized
     * before, false otherwise.
     * @throws PartitionMDException if fail to scan the database
     */
    public synchronized boolean isTableInitBefore()
        throws PartitionMDException {

        return !dbManager.isEmpty();
    }

    /**
     * Scans the db and init in memory generation table. It marks generation
     * table ready only if found INIT_DONE_MARKER from database. This method
     * should only be used by initialize(), which is called on master of
     * source and target shard before partition migrations. Because this
     * method may update database while adding the generations to the in-memory
     * table.
     */
    private synchronized void scanDB() throws PartitionMDException {

        final Database dbHandle = repNode.getMigrationManager()
                                         .getGenerationDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open on rn=" +
                                            repNode.getRepNodeId());
        }

        final List<PartitionGeneration> allHistory = dbManager.scan();
        if (allHistory != null) {
            for (PartitionGeneration pg : allHistory) {

                /* found INIT_DONE_MARKER, initialization is done */
                if (pg.getPartId().equals(
                    PartitionGeneration.getInitDoneMarkerId())) {
                    ready = true;
                    continue;
                }
                addGeneration(pg);
            }
        }

        logger.fine(() -> lm("Generation table initialized" +
                             ", #partitions=" + genTable.size() +
                             ", #open=" + getOpenGens().size() +
                             ", open=" + Arrays.toString(
                                 getOpenGens().keySet().toArray())));
    }

    /**
     * Closes the partition generation table
     */
    public void close() {
        if (!ready) {
            return;
        }
        ready = false;
        genTable.clear();
        logger.info(lm("Partition generation table closed"));
    }

    /**
     * Returns the highest VLSN of all closed generations in the table, or
     * {@link VLSN#NULL_VLSN} if no generation is closed.
     *
     * @return the highest VLSN of all closed generations or
     * {@link VLSN#NULL_VLSN}
     */
    public synchronized long getHighVLSNClosedGen() {
        return genTable.keySet().stream()
                       .map(this::getLastGen)
                       .filter(pg -> !pg.isOpen())
                       .mapToLong(PartitionGeneration::getEndVLSN)
                       .max()
                       .orElse(NULL_VLSN);
    }

    /**
     * Generate strings to dump the complete partition generation table
     */
    synchronized String dumpTable() {
        final StringBuilder sb = new StringBuilder();
        if (isReady()) {
            sb.append("Open generations=")
              .append(getOpenGens().keySet()).append("\n");
            sb.append("List of all generations=\n");
            genTable.values().forEach(hist -> {
                sb.append(hist.values());
                sb.append("\n");
            });
        } else {
            sb.append("Unavailable");
        }
        return sb.toString();
    }

    /* query the table */

    /**
     * Returns true if the underlying JE base contains the generation of given
     * partition, either open or closed.
     *
     * @param pid partition id
     * @return true if the table has the partition
     * @throws IllegalStateException if called before the partition generation
     * table is initialized and ready
     */
    //TODO: Remove synchronized -- should not be needed
    public synchronized boolean hasPartition(PartitionId pid)
        throws IllegalStateException {

        if (!ready) {
            throw new IllegalStateException("Partition generation table is " +
                                            "not available");
        }

        return genTable.containsKey(pid);
    }

    /**
     * Returns the generation for a partition with specific generation
     * number, or null if the record does not exist in database
     *
     * @param pid id of partition
     * @param pgn generation number of a partition
     * @return the generation for a partition with specific generation
     * number, or null
     */
    // TODO: Remove synchronized -- should not be needed
    synchronized PartitionGeneration getGen(PartitionId pid,
                                            PartitionGenNum pgn) {

        if (!ready) {
            throw new IllegalStateException("Partition generation table is " +
                                            "not available");
        }

        final Map<PartitionGenNum, PartitionGeneration> his = genTable.get(pid);
        if (his == null) {
            return null;
        }

        return his.get(pgn);
    }

    /**
     * Returns the set of generations hosted by RN at the given VLSN, or
     * empty set if no generation covers the given VLSN.  Note for each
     * partition, at most one generation can cover the given vlsn.
     *
     * @param vlsn VLSN
     * @return a set of generations hosted by RN at the given VLSN, sorted by
     * the generation number
     */
    synchronized Set<PartitionGeneration> getGensWithVLSN(long vlsn) {
        if (!ready) {
            throw new IllegalStateException("Partition generation table is " +
                                            "not available");
        }

        if (vlsn == INVALID_VLSN || VLSN.isNull(vlsn)) {
            throw new IllegalArgumentException("VLSN cannot be null");
        }

        return genTable.values().stream()
                       .flatMap(m -> m.values().stream())
                       .filter(pg -> pg.inGeneration(vlsn))
                       .collect(Collectors.toSet());
    }

    /* update the table */

    /**
     * Opens a generation for a given partition with given generation number
     * at a start vlsn. The returned partition generation should be added to
     * the in-memory structures if the transaction is committed.
     *
     * @param pid          partition id, should not be null or NULL_ID
     * @param pgn          partition generation number, should not be zero,
     *                     which is used for the first generations that are
     *                     only opened at time of the initialization.
     * @param prevGenShard shard of previous generation, null if very first
     *                     generation
     * @param prevLastVLSN last vlsn of previous generation, if exists
     * @param txn          parent txn if called within a txn, must not be null
     * @return the opened partition generation
     * @throws PartitionMDException if fail to open a partition generation
     */
    public synchronized
    PartitionGeneration openGeneration(PartitionId pid,
                                       PartitionGenNum pgn,
                                       RepGroupId prevGenShard,
                                       long prevLastVLSN,
                                       Transaction txn)
        throws PartitionMDException {
        if (!ready) {
            throw new IllegalStateException("Partition generation table is " +
                                            "not available");
        }
        if (pid == null || pid.isNull()) {
            throw new IllegalArgumentException("partition id cannot be null");
        }
        if (txn == null) {
            throw new IllegalArgumentException("Must specify a transaction to" +
                                               "open a generation");
        }
        if (pgn.equals(PartitionGenNum.generationZero())) {
            throw new IllegalStateException(
                "Partition generation number should not be zero");
        }

        if (pgn.getNumber() > 0 && prevGenShard.isNull()) {
            throw new IllegalArgumentException("Must be specify a valid " +
                                               "previous shard for " +
                                               "non-zero generation number=" +
                                               pgn);
        }

        final PartitionGeneration openPG = getOpenGen(pid);
        if (openPG != null) {
            throw new IllegalStateException("Pid=" + pid + " already has an " +
                                            "open generation=" + openPG);
        }

        if (hasPartition(pid)) {
            final PartitionGenNum lastGenNum = getHistory(pid).lastKey();
            /* new PGN must be greater than any previous generation */
            if (pgn.compareTo(lastGenNum) <= 0) {
                throw new IllegalStateException("Pid=" + pid + " " +
                                                "has the last generation #=" +
                                                lastGenNum + " newer than" +
                                                " given #=" + pgn);
            }
        }

        /*
         * open an new generation, start VLSN is the latest VLSN from
         * VLSN index, write into JE database
         */
        final PartitionGeneration pg = new PartitionGeneration(
            pid, pgn, getLastVLSN(), prevGenShard, prevLastVLSN);

        /* persist to je database */
        try {
            dbManager.put(txn, pid, pg);
            logger.info(lm("Persisted an open generation=" + pg));
        } catch (PartitionMDException pmde) {
            logger.warning(lm("Fail to open generation=" + pg +
                              ", error=" + pmde));
            /* let caller deal with it */
            throw pmde;
        }
        return pg;
    }

    /**
     * Creates the very first open generation for given set of partitions for
     * partition generation table initialization, caller must check if the
     * INIT_DONE_MARKER exists before open the first generations.
     *
     * @param pids set of partition id
     */
    private synchronized void openFirstGeneration(Set<PartitionId> pids)
        throws PartitionMDException {

        Transaction txn = null;
        try {
            txn = dbManager.beginTransaction();
            for (PartitionId pid : pids) {
                final PartitionGeneration pg = new PartitionGeneration(pid);
                /* persist to je database */
                dbManager.put(txn, pid, pg);

                /* update in-memory structure */
                addOpenGen(pg);
                logger.info(lm("Added very first open generation=" + pg));
            }

            /* persist initDone marker */
            assert TestHookExecute.doHookIfSet(beforeInitDoneHook, null);
            dbManager.put(txn, PartitionId.NULL_ID, INIT_DONE_MARKER);
            txn.commit();
            txn = null;
            logger.fine(() -> lm("Done initialize owned partition"));
        } catch (PartitionMDException pmde) {
            logger.warning(lm("Fail to open first generation for partitions" +
                              pids + ", error=" + pmde));

            /* let caller deal with it */
            throw pmde;
        } finally {
            TxnUtil.abort(txn);
            if (txn != null) {
                pids.forEach(pid -> genTable.remove(pid));
            }
        }
    }

    /**
     * Closes an open generation with end VLSN. Usually called when a
     * partition migrates out of a shard.
     *
     * @param pid     partition id
     * @param endVLSN end vlsn to close the generation
     * @param txn     txn in which the persistence will be made, if null, a
     *                new txn will be created
     */
    public synchronized void closeGeneration(PartitionId pid,
                                             long endVLSN,
                                             Transaction txn)
        throws PartitionMDException {

        /*
         * Close generation may be called during initialization from within
         * {@link PartitionGenerationTable#initialize()}, therefore we should
         * not throw exception if ready=false here since it is always false
         * during initialization(). Instead, log flag and partition for
         * diagnosis.
         */
        if (!ready) {
            logger.info(lm("During initialization, to close generation for " +
                           "partition=" + pid + ", endVLSN=" + endVLSN));
        }

        /* get the last generation */
        PartitionGeneration pg = getLastGen(pid);
        if (pg == null) {
            throw new IllegalArgumentException("No generation exists for pid=" +
                                               pid);
        }

        /*
         * In partition migration, there is a window on the target where a
         * failure can occur between receiving the EoD and the partition made
         * permanent, in this case the partition be re-instated on the source
         * even the EoD has been sent successfully. That means, we may end up
         * close a generation and update the end VLSN multiple times, until
         * the migration is completely over.
         */
        if (!pg.isOpen()) {
            final String msg = "Pid=" + pid + " generation=" + pg +
                               " already closed at vlsn=" + pg.getEndVLSN() +
                               ", new end vlsn=" + endVLSN;
            logger.info(lm(msg));
        }

        /* make a clone of open generation  */
        final PartitionGeneration pgClone =
            new PartitionGeneration(pg.getPartId(), pg.getGenNum(),
                                    pg.getStartVLSN(), pg.getPrevGenRepGroup(),
                                    pg.getPrevGenEndVLSN());
        pgClone.close(endVLSN);
        /*
         * If all partitions are closed on this shard, mark in the
         * generation to pass the info to stream client in rep stream
         */
        final Map<PartitionId, PartitionGeneration> openPids = getOpenGens();
        if (openPids.size() == 1 && openPids.containsKey(pg.getPartId())) {
            /*
             * pg is the last partition in the open generation, mark it and
             * let client close the RSC
             */
            pgClone.setAllPartClosed();
            logger.info(lm("All partitions closed after closing pid=" + pid));
        } else {
            logger.fine(() -> lm("Closing pid=" + pg.getPartId() +
                                 ", open pids=" + openPids));
        }

        dbManager.put(txn, pid, pgClone);

        /* close in-memory structure after db updated successfully */
        pg.close(endVLSN);
        logger.info(lm("Pid=" + pid + " has closed generation #=" +
                       pgClone.getGenNum() + " at vlsn=" + endVLSN +
                       ", flag ready=" + ready));
        logger.fine(() -> lm(dumpTable()));
    }

    /**
     * Reopen the last generation of a given partition. The operation is made
     * in the caller provided transaction. The returned partition generation
     * should be added to the in-memory structures if the transaction is
     * committed.
     *
     * @param pid partition id
     * @param txn parent txn
     * @return true if generation reopened, false otherwise
     */
    public synchronized boolean
    reOpenLastGeneration(PartitionId pid, Transaction txn) {
        final PartitionGeneration closedGen = getLastGen(pid);
        if (closedGen.isOpen()) {
            /* possible that a generation may be reopened */
            logger.info(lm("Already open generation=" + closedGen));
            return true;
        }

        /* build an open gen from the closed */
        final PartitionGeneration openGen =
            new PartitionGeneration(closedGen.getPartId(),
                                    closedGen.getGenNum(),
                                    closedGen.getStartVLSN(),
                                    closedGen.getPrevGenRepGroup(),
                                    closedGen.getPrevGenEndVLSN());

        /* persist to je database */
        boolean ret = false;
        try {
            /* persistence */
            dbManager.put(txn, pid, openGen);
            /* update in-mem structure */
            addOpenGen(openGen);
            logger.info(lm("Reopened generation=" + openGen));
            ret = true;
        } catch (PartitionMDException pmde) {
            logger.warning(lm("Fail to reopen generation=" + openGen +
                              ", error=" + pmde));
        }
        return ret;
    }

    /**
     * Returns the last VLSN from VLSN index
     *
     * @return the last VLSN from VLSN index
     */
    public long getLastVLSN() {
        return dbManager.getLastVLSN();
    }

    /**
     * Returns the open generation for given partition, or null if no open
     * generation exists for given partition
     *
     * @param pid partition id
     * @return the open generation for given partition, or null
     */
    public PartitionGeneration getOpenGen(PartitionId pid) {
        if (!ready) {
            throw new IllegalStateException("Partition generation table is " +
                                            "not available");
        }

        final PartitionGeneration ret = getLastGen(pid);
        if (ret == null) {
            logger.fine(() -> lm("Generation does not exist for pid=" + pid));
            return null;
        }

        if (!ret.isOpen()) {
            /*
             * Find a closed generation for the partition, it could happen when
             * a previously migrated out partition migrates back to the shard.
             */
            logger.fine(() -> lm("Previously closed generation=" + ret));
            return null;
        }
        return ret;
    }

    /**
     * Returns a list of generation records for a given partition id, or null
     * if the table does not have any record the partition.
     *
     * @param pid partition id
     * @return a sorted set of generation records for a given partition id, or
     * null if the table does not have any record the partition.
     */
    public SortedMap<PartitionGenNum, PartitionGeneration>
    getHistory(PartitionId pid) {
        if (!ready) {
            throw new IllegalStateException("Partition generation table is " +
                                            "not available");
        }

        return genTable.get(pid);
    }

    /**
     * Returns the last generation for a given partition, or null if the
     * table does not have any record for the given partition.
     *
     * @param pid given partition id
     * @return the last generation for a given partition
     */
    public PartitionGeneration getLastGen(PartitionId pid) {
        final SortedMap<PartitionGenNum, PartitionGeneration> history =
            genTable.get(pid);
        if (history == null) {
            return null;
        }
        return history.get(history.lastKey());
    }

    /**
     * Returns the db manager
     */
    PartitionGenDBManager getDbManager() {
        return dbManager;
    }

    /**
     * Adds a generation to the table
     *
     * @param pg partition generation
     */
    public synchronized void addGeneration(PartitionGeneration pg) {
        if (pg == null) {
            return;
        }

        final PartitionId pid = pg.getPartId();
        final PartitionGeneration last = getLastGen(pid);
        /* a new partition */
        if (last == null) {
            addOpenGen(pg);
            logger.info(lm("Partition=" + pid + " does not exist, " +
                           "added an open generation=" + pg));
            return;
        }

        /* a previously closed partition */
        if (!last.isOpen()) {
            addOpenGen(pg);
            logger.info(lm("Partition=" + pid + " previously " +
                           "closed= " + last + ", adding an open " +
                           "generation=" + pg));
            return;
        }

        /* an existing open generation */
        if (last.getGenNum().getNumber() >= pg.getGenNum().getNumber()) {
            logger.info(lm("Partition=" + pg.getPartId() +
                           " already has an open gen#=" + last.getGenNum() +
                           " higher than gen#=" + pg.getGenNum() +
                           ", existing={" + last + "}, ignored={" + pg + "}"));
            return;
        }

        /*
         * a bit strange, but there is an open generation with smaller
         * generation number. this may happen when the
         * {@link oracle.kv.impl.rep.PartitionManager#updateThread} is
         * reopening the closed generation when a previous partition migrates
         * back to the shard. In this case, we just close the opened
         * generation, and add the new one with higher generation number.
         */
        logger.info(lm("Exists an open gen#=" + last.getGenNum().getNumber() +
                       " smaller than gen#=" + pg.getGenNum().getNumber() +
                       ", closing existing gen=" + last));
        closeGeneration(pg.getPartId(), pg.getStartVLSN(), null);
        addOpenGen(pg);
        logger.info(lm("Added an open gen=" + pg));
    }

    private synchronized void addOpenGen(PartitionGeneration pg) {
        genTable.computeIfAbsent(
            pg.getPartId(),
            /* make it thread-safe */
            u -> Collections.synchronizedSortedMap(new TreeMap<>()))
                .put(pg.getGenNum(), pg);
        logger.fine(() -> lm("Adding open generation=" + pg + dumpTable()));
    }

    /**
     * Returns a list of open generations in table
     */
    private synchronized Map<PartitionId, PartitionGeneration> getOpenGens() {
        return genTable.values().stream()
                       .flatMap(map -> map.values().stream())
                       .filter(PartitionGeneration::isOpen)
                       .collect(Collectors.toMap(PartitionGeneration::getPartId,
                                                 pg -> pg));
    }

    /**
     * Initializes the partition generation table. The db must exist, scan the
     * db and populate the in-memory structure.
     * <p>
     * After this call,
     * - partition generation db is opened and scanned
     * - all owned partitions are opened
     * - all in-memory structures are initialized
     * - all stream feeder filters are notified if exist
     */
    public synchronized void initialize() {

        if (ready) {
            /* avoid re-init the table */
            logInitDone();
            return;
        }

        if (genTable == null) {
            genTable = new ConcurrentHashMap<>();
        }

        /* init the table from generation db */
        scanDB();

        if (ready) {
            /* found INIT_DONE_MARKER in scanDB, no need to re-init */
            logInitDone();
            return;
        }

        /* let each stream filter know and initialize the owned partition */
        final Topology topo = repNode.getTopology();
        final int gid = repNode.getRepNodeId().getGroupId();
        final Set<PartitionId> ownedParts = getOwnedParts(topo, gid);
        /*
         * open every partition owned by the RN
         *
         * if a new db is created we need open every owned partitions; if the
         * db already exists, e.g., a replica promoted to master and runs
         * migrations, the new master may already has the generation db and
         * opened generations, in this case, we check if any missing
         * partition in the db and open it if any.
         */
        final Set<PartitionId> opened = new HashSet<>();
        final Set<PartitionId> exists = new HashSet<>();
        ownedParts.forEach(pid -> {
            if (genTable.containsKey(pid)) {
                exists.add(pid);
            } else {
                opened.add(pid);
            }
        });
        openFirstGeneration(opened);

        /* table is initialized */
        ready = true;

        /*
         * Let feeder know that db is created and construct the white list.
         * Only notify feeder at first initialization that generation database
         * is empty. Re-initialization adds missing partitions in database, the
         * existing stream filter processes the commits and add them. The new
         * filter reads from database at the startup.
         */
        if (exists.isEmpty()) {
            notifyFeederFilter(ownedParts);
        }

        logger.info(lm("Generation table initialized with owned partitions=" +
                       ownedParts.stream()
                                 .map(PartitionId::getPartitionId)
                                 .collect(Collectors.toSet())));
        logger.fine(() -> lm("Generations already existed in db (last gen)=" +
                             exists.stream().map(this::getLastGen)
                                   .collect(Collectors.toSet()) +
                             ", generations opened=" +
                             opened.stream().map(this::getLastGen)
                                   .collect(Collectors.toSet())));

    }

    private void logInitDone() {
        logger.fine(() -> {
            final Set<PartitionId> pids = getOpenGens().keySet();
            return lm("Generation table (#partitions=" + genTable.size() +
                      ") has been already initialized. " +
                      "#open=" + pids.size() + ", " +
                      "pid of open=" + Arrays.toString(pids.toArray()));
        });
    }

    /**
     * Initialize the in-memory partition generation table from database.
     * The in-memory table will only be initialized if it's null, otherwise
     * do nothing and return.
     */
    public synchronized void initFromDatabase() {
        if (genTable != null) {
            return;
        }
        logger.info(lm("Initialize partition generation table from db"));
        refreshTableFromDB();
    }

    /**
     * Refresh the in-memory partition generation table and states
     * from generation database.
     */
    public synchronized void refreshTableFromDB() {
        final Database dbHandle = repNode.getMigrationManager()
                                         .getGenerationDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open on rn=" +
                                            repNode.getRepNodeId());
        }

        final ConcurrentMap<PartitionId,
            SortedMap<PartitionGenNum, PartitionGeneration>> newTable =
            new ConcurrentHashMap<>();
        boolean isReady = false;

        /*
         * Scan generation database to see if it is initialized, and read
         * all existing generation history to populate newTable.
         */
        final List<PartitionGeneration> allHistory = dbManager.scan();
        if (allHistory != null) {
            for (PartitionGeneration pg : allHistory) {
                if (pg.getPartId().equals(
                    PartitionGeneration.getInitDoneMarkerId())) {
                    isReady = true;
                    continue;
                }
                newTable.computeIfAbsent(
                    pg.getPartId(),
                    u -> Collections.synchronizedSortedMap(new TreeMap<>()))
                        .put(pg.getGenNum(), pg);
            }
        }

        if (logger.isLoggable(Level.INFO) && (genTable != null)) {
            final List<PartitionGeneration> added = new ArrayList<>();
            for (PartitionId pid : newTable.keySet()) {
                final SortedMap<PartitionGenNum, PartitionGeneration> newGens =
                    newTable.get(pid);
                if (!genTable.containsKey(pid)) {
                    added.add(newGens.get(newGens.lastKey()));
                    continue;
                }
                final SortedMap<PartitionGenNum, PartitionGeneration> oldGens =
                    genTable.get(pid);
                if (newGens.size() != oldGens.size()) {
                    added.add(newGens.get(newGens.lastKey()));
                }
            }
            if (!added.isEmpty()) {
                logger.info("Added new partition generations: " +
                            added.stream().limit(10)
                            .map(pg -> String.format("<%s>", pg))
                            .collect(
                                Collectors.joining(
                                    ",", "[",
                                    added.size() > 10 ? "...]" : "]")));
            }
        }

        /* update cached table and state */
        genTable = newTable;
        ready = isReady;

        logger.fine(() -> {
            final StringBuilder sb = new StringBuilder();
            newTable.values().forEach(hist -> {
                sb.append(hist.values());
                sb.append("\n");
            });
            return lm("Refreshed partition generation table," +
                      "current generations are:\n" + sb +
                      ", flag ready=" + ready);
        });
    }

    /**
     * Return whether the generation of given partition is opened on this RN.
     * When there wasn't partition migration, this method always return true
     * for given partition.
     *
     * @param pid partition id
     * @return true if given partition is open on this RN, false otherwise
     */
    public boolean isPartitionOpen(PartitionId pid) {
        if (genTable == null) {
            throw new IllegalStateException("Partition table is not available");
        }

        /*
         * Generation table hasn't been initialized, no migrations
         * before, return true for all given partitions.
         */
        if (!ready) {
            return true;
        }

        /* Check if given partition is open in the generation table */
        final PartitionGeneration ret = getLastGen(pid);
        return ret != null && ret.isOpen();
    }

    /* test use only */
    ConcurrentMap<PartitionId,
        SortedMap<PartitionGenNum, PartitionGeneration>> getGenTable() {

        return genTable;
    }

    /**
     * Notifies each stream feeder filter that a partition generation db
     * has been created.
     *
     * @param ownedParts list of partitions owned by the RN
     */
    private void notifyFeederFilter(Set<PartitionId> ownedParts) {

        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        if (repEnv == null) {
            logger.info(lm("Rep env closed on rn=" + repNode.getRepNodeId()));
            return;
        }

        final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
        final com.sleepycat.je.rep.impl.node.RepNode rn = repImpl.getRepNode();
        if (rn == null || !rn.isMaster() || rn.feederManager() == null) {
            return;
        }

        /* notify each stream feeder filter for external node  */
        final String dbName = PartitionGenDBManager.getDBName();
        final DatabaseId dbId = FeederReplicaSyncup.getDBId(repImpl, dbName);
        rn.feederManager().activeReplicasMap().values().stream()
          /* look for external node feeder with stream filter */
          .filter(f -> f.getReplicaNode().getType().isExternal() &&
                       f.getFeederFilter() != null &&
                       f.getFeederFilter() instanceof NoSQLStreamFeederFilter)
          .forEach(f -> {
              final NoSQLStreamFeederFilter strFilter =
                  (NoSQLStreamFeederFilter) f.getFeederFilter();
              strFilter.initPartGenTableFromMigration(dbId, ownedParts,
                                                      repImpl);
              logger.info(lm("Notify stream filter for replica=" +
                             f.getReplicaNode().getName() +
                             ", owned partitions=" + ownedParts));
          });
    }

    /**
     * Returns the list of partition ids owned by the given rep group
     *
     * @param topo  topology
     * @param gid   replication group id
     *
     * @return list of owned partitions, or
     */
    private Set<PartitionId> getOwnedParts(Topology topo, int gid) {
        final PartitionMap pmap = topo.getPartitionMap();
        return pmap.getAllIds().stream()
                   .filter(pid -> pmap.getRepGroupId(pid).getGroupId() == gid)
                   .collect(Collectors.toSet());
    }

    private String lm(String msg) {
        return "[PGT-" + repNode.getRepNodeId() + "] " + msg;
    }

    void setBeforeInitDoneHook(TestHook<Integer> hook) {
        this.beforeInitDoneHook = hook;
    }

    /**
     * Closes the partition generation.
     *
     * @param partitionId partition id
     * @return true if generation closed successfully, false otherwise
     */
    public boolean closePartGenInTxn(PartitionId partitionId) {

        final ReplicatedEnvironment repEnv = repNode.getEnv(1);
        if (repEnv == null) {
            logger.info(lm("Replicated env unavailable, cannot close " +
                           "generation for partition=" + partitionId));
            return false;
        }
        final PartitionManager pm = repNode.getPartitionManager();
        final PartitionGenerationTable pgt = pm.getPartGenTable();
        final PartitionGeneration pg = pgt.getLastGen(partitionId);
        if (pg == null || !pg.isOpen()) {
            /* generation does not exist or already closed */
            return true;
        }

        /* close the generation */
        final long endVLSN = pgt.getLastVLSN();
        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setDurability(
            new Durability(Durability.SyncPolicy.SYNC,
                           Durability.SyncPolicy.SYNC,
                           Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        Transaction txn = null;
        try {
            txn = repEnv.beginTransaction(null, txnConfig);
            /* close generation both in memory and on-disk */
            pgt.closeGeneration(partitionId, endVLSN, txn);
            txn.commit();
            txn = null;
            return true;
        } catch (RuntimeException ex) {
            logger.warning(lm("Fail to close generation=" + pg +
                              ", endVLSN=" + endVLSN +
                              ", error=" + ex));
            return false;
        } finally {
            TxnUtil.abort(txn);
        }
    }
}