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

package oracle.kv.impl.rep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.Key;
import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.map.KeyToPartitionMap;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.migration.MigrationManager;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.SecondaryAssociation;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Manages the partition database handles for the rep node.
 */
public class PartitionManager {

    /**
     * The amount of time to wait between retries when updating partition db
     * data structures and checking for generation readiness.
     */
    public static final int DB_UPDATE_RETRY_MS = 1000;

    /** A test hook that delays the partition handler update. */
    public static volatile TestHook<UpdateTopoHookObject>
        updateTopoTestHook;

    public static class UpdateTopoHookObject {
        public final RepNode repNode;
        public final Topology topology;

        public UpdateTopoHookObject(RepNode repNode,
                          Topology topology) {
            this.repNode = repNode;
            this.topology = topology;
        }

        @Override
        public String toString() {
            return String.format(
                "%s update for topology %s",
                repNode.getRepNodeId(),
                topology.getOrderNumberTuple());
        }
    }

    /**
     * The number of partitions to display for the update logging messages.
     */
    private static final int UPDATE_DISPLAY_LIMIT = 10;

    private final RepNode repNode;

    /**
     * The database configuration used to create and access partition
     * databases.
     */
    private final DatabaseConfig partitionDbConfig;

    private final Logger logger;
    private final RateLimitingLogger<String> rateLimitingLogger;

    /**
     * A map from partitionId to the canonical partition database handle. Once
     * initialized, the map is only modified as a result of partition
     * migration.
     */
    private final Map<PartitionId, Database> partitionDbMap =
                        new ConcurrentHashMap<>();

    /*
     * Maps a key to a partition id. Set the first time the DB handles are
     * updated.
     */
    private volatile KeyToPartitionMap mapper = null;

    /**
     * Tracks the updating state.
     */
    private final UpdateStateTracker updateStateTracker =
        new UpdateStateTracker();

    private UpdateThread updateThread = null;

    /* Partition generation table, or null if not initialized */
    private volatile PartitionGenerationTable partGenTable = null;

    PartitionManager(RepNode repNode,
                     SecondaryAssociation secondaryAssociation,
                     Params params) {
        this.repNode = repNode;
        partitionDbConfig =
            new DatabaseConfig().setTransactional(true).
                                 setAllowCreate(true).
                                 setBtreeByteComparator(
                                     Key.BytesComparator.class).
                                 setKeyPrefixing(true).
                                 setSecondaryAssociation(secondaryAssociation).
                                 setCacheMode(
                                    params.getRepNodeParams().getJECacheMode());
        logger = LoggerUtils.getLogger(this.getClass(), params);
        logger.log(Level.INFO,
                   "Partition database cache mode: {0}",
                   partitionDbConfig.getCacheMode());
        rateLimitingLogger = new RateLimitingLogger<String>(
            5 * 60 * 1000 /* 5 minutes */,
            10 /* types of limit objects */,
            logger);
    }

    /**
     * Returns the partition Db config
     *
     * @return the partition Db config
     */
    DatabaseConfig getPartitionDbConfig() {
        return partitionDbConfig;
    }

    Set<PartitionId> getPartitions() {
        return partitionDbMap.keySet();
    }

    /**
     * Initialize the in-memory partition generation table. The table is
     * initialized from partition generation database if it doesn't exist.
     */
    synchronized void initPartGenTable() {
        if (partGenTable != null) {
            return;
        }
        partGenTable = new PartitionGenerationTable(repNode, logger);
        partGenTable.initFromDatabase();
    }

    /**
     * Asynchronously opens the partition database handles associated with the
     * partitions stored at this rep node. If an update thread is running it is
     * not restarted, avoiding any wait for the thread to stop.
     */
    synchronized void updateDbHandles(Topology topology) {

        /* If an update is already in progress just let it continue. */
        if (updateThread != null) {
            return;
        }
        final ReplicatedEnvironment repEnv = repNode.getEnv(1);

        if (repEnv != null) {
            updateDbHandles(topology, repEnv);
        }
    }

    /**
     * Asynchronously opens the partition database handles associated with the
     * partitions stored at this rep node. The databases are created if they do
     * not already exist. All database handles are cached in the partitionDbMap
     * for use by subsequent partition level operations.
     * <p>
     * This method is invoked at startup. At this time, new databases may be
     * created if this node is the master and databases need to be created for
     * the partitions assigned to this node. If the node is a replica, it may
     * need to wait until the databases created on the master have been
     * replicated to it.
     * <p>
     * Post startup, this method is invoked to re-establish database handles
     * whenever the associated environment handle is invalidated and needs to
     * be re-established. Or via the TopologyManager's listener interface
     * whenever the Topology has been updated.
     *
     * @param topology the topology describing the current
     * @param repEnv the replicated environment handle
     */
    synchronized void updateDbHandles(Topology topology,
                                      ReplicatedEnvironment repEnv) {
        assert topology != null;
        assert repEnv != null;

        stopUpdate();

        if (partGenTable == null) {
            throw new IllegalStateException(
                "Partition generation table must be initialized " +
                "before update db handles");
        }
        updateThread = new UpdateThread(topology, repEnv);
        updateThread.start();

        /* Set the mapper if the number if partitions have been determined. */
        if (!isInitialized()) {
            final int nPartitions = topology.getPartitionMap().getNPartitions();

            if (nPartitions > 0) {
                mapper = new HashKeyToPartitionMap(nPartitions);
            }
        }
    }

    /**
     * Stops the updater and waits for its thread to exit.
     */
    private void stopUpdate() {
        assert Thread.holdsLock(this);

        if (updateThread != null) {
            updateThread.waitForStop();
            updateThread = null;
        }
    }

    /**
     * Closes all partition DB handles, typically as a precursor to closing the
     * environment handle itself. The caller is assumed to have made provisions
     * if any to ensure that the handles are no longer in use.
     */
    synchronized void closeDbHandles() {
        logger.log(Level.INFO, "Closing partition database handles");

        stopUpdate();

        /*
         * Note that closing databases will terminate any operations that
         * are in progress for that partition.
         */
        for (Database pDb : partitionDbMap.values()) {
            if (!closePartitionDB(pDb)) {
                /* Break out on an env failure */
                return;
            }
        }

        /* Close generation table after closing partition databases */
        if (partGenTable != null) {
            partGenTable.close();
        }
    }

    /**
     * Closes a partition DB, handling exceptions. Returns true if there were
     * no environment failures.
     *
     * @param pDb a partition database
     * @return true if the environment associated with the DB was invalid or
     * closed
     */
    private boolean closePartitionDB(Database pDb) {
        final Environment env = pDb.getEnvironment();

        if ((env == null) || !env.isValid()) {
            return false;
        }

        TxnUtil.close(logger, pDb, "partition");
        return true;
    }

    /**
     * Returns the partition associated with the key
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the partitionId associated with the key
     */
    public PartitionId getPartitionId(byte[] keyBytes) {
        return mapper.getPartitionId(keyBytes);
    }

    /**
     * Returns the partition generation table
     *
     * @return the partition generation table
     */
    public PartitionGenerationTable getPartGenTable() {
        return partGenTable;
    }

    /**
     * Returns the database associated with the key. Returns null if the
     * key is not associated with a partition on this node.
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the database associated with the key or null
     */
    Database getPartitionDB(byte[] keyBytes) {
        return partitionDbMap.get(mapper.getPartitionId(keyBytes));
    }

    /**
     * Returns the database associated with the partition. Returns null if the
     * partition is not on this node.
     *
     * @param partitionId the partition used for looking up the database.
     *
     * @return the database associated with the partition or null
     */
    public Database getPartitionDB(PartitionId partitionId) {
        return partitionDbMap.get(partitionId);
    }

    /**
     * Returns true if the database for the specified partition is open.
     *
     * @param partitionId a partition ID
     * @return true if the database for the specified partition is open
     */
    boolean isPresent(PartitionId partitionId) {
        return partitionDbMap.containsKey(partitionId);
    }

    /**
     * Returns true if the manager has been initialized.
     *
     * @return true if the manager has been initialized
     */
    boolean isInitialized() {
        return mapper != null;
    }

    /**
     * Waits for partition data structures and contents to be updated to
     * reflect the specified topology or the ones afterwards with respect to
     * the localization order (see Topology#getLocalizationNumber). Returns the
     * current topology that is either the specified topology or one afterwards
     * in order, or {@code null} if it timed out.
     */
    public Topology awaitUpdate(
        Topology targetTopo, long timeoutMillis)
        throws InterruptedException
    {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                String.format("non-positive timeout value: %s", timeoutMillis));
        }
        final long currTimeMillis = System.currentTimeMillis();
        final long stopTimeMillis =
            (currTimeMillis + timeoutMillis <= 0)
            ? Long.MAX_VALUE : currTimeMillis + timeoutMillis;
        while (true) {
            /*
             * Below, we continue waiting until the updateState meets our
             * requirement or timeout, and expect an InterruptedException if
             * the whole process is shutting down. Alternatively, we could
             * implement to stop waiting if there is no more UpdateThread
             * running. However, we have to deal with the distinction of the
             * UpdateThread is being temporarily stopped by the next update as
             * opposed to the whole partition manager being shut down. This
             * seems not worth the effort.
             */
            synchronized(updateStateTracker) {
                final UpdateState updateState =
                    updateStateTracker.getUpdateState();
                if (updateState.includesUpdate(targetTopo)) {
                    return updateState.latestUpdateTopology;
                }
                final long waitMillis =
                    stopTimeMillis - System.currentTimeMillis();
                if (waitMillis <= 0) {
                    return null;
                }
                updateStateTracker.wait(waitMillis);
            }
        }
    }

    /**
     * Tracks the state of the update. All accesses are synchronized under
     * its object lock.
     */
    private class UpdateStateTracker {
        /**
         * The latest topology supplied by an update, or null if no updates.
         * Only valid if non-null and the updateInProgress field is false,
         * otherwise either prior to the first update or in the process of
         * updating to a new topology.
         */
        private Topology latestUpdateTopology = null;

        /**
         * Whether a topology update is currently underway.
         */
        private boolean updateInProgress = false;

        /**
         * Returns {@code true} and marks the start of an update if the
         * specified topology is a needed update, otherwise returns {@code
         * false}.
         */
        private synchronized boolean maybeStartUpdate(Topology targetTopo) {
            logger.fine(
                () ->
                String.format(
                    "Partition update check maybeStartUpdate for "
                    + "topology %s, "
                    + "latest update topology %s",
                    targetTopo.getOrderNumberTuple(),
                    (latestUpdateTopology == null)
                    ? "null" : latestUpdateTopology.getOrderNumberTuple()));
            if ((latestUpdateTopology == null)
                /*
                 * Note that we will start the update if the targetTopo is the
                 * same as the latestUpdateTopology. This is important because
                 * the environment may be restarted after a completed update
                 * and in that case we need to update again to open the
                 * databases. We can add an extra flag to indicate if we are
                 * under a restart, but it seems safer to me to just allow
                 * update for the same topology.
                 */
                || (compareOrder(latestUpdateTopology, targetTopo) <= 0)) {
                updateInProgress = true;
                return true;
            }
            return false;
        }

        /** Marks the finish of an update of a topology. */
        private synchronized void updateComplete(Topology topology) {
            latestUpdateTopology = topology;
            updateInProgress = false;
            notifyAll();
        }

        /** Returns the update state. */
        private synchronized UpdateState getUpdateState() {
            return new UpdateState(
                latestUpdateTopology,
                updateInProgress);
        }
    }

    /**
     * Describes the state of the latest update.
     */
    private class UpdateState {
        private final Topology latestUpdateTopology;
        private final boolean updateInProgress;

        private UpdateState(Topology latestUpdateTopology,
                            boolean updateInProgress) {
            this.latestUpdateTopology = latestUpdateTopology;
            this.updateInProgress = updateInProgress;
        }

        /**
         * Checks if the partition map is up-to-date relative to the provided
         * topology, which can be either an official or localized topology. A
         * partition is up-to-date if its DB is open and its data is up-to-date
         * relative to its generation.
         */
        private boolean includesUpdate(Topology topo) {
            if (updateInProgress) {
                return false;
            }
            if (latestUpdateTopology == null) {
                return false;
            }
            return compareOrder(topo, latestUpdateTopology) <= 0;
        }

        @Override
        public String toString() {
            return String.format(
                "latestUpdateTopo=%s, updateInProgress=%s",
                (latestUpdateTopology == null)
                ? "null" : latestUpdateTopology.getOrderNumberTuple(),
                updateInProgress);
        }
    }

    /**
     * Compares the two topology for order. Returns a negative integer, zero,
     * or a positive integer as the specified topology {@code t1} is ordered
     * before, the same or after the topology {@code t2}. The order of the
     * topology is defined with the tuple (sequenceNumber, localizationNumber)
     * of the topology object.
     */
    private int compareOrder(Topology t1, Topology t2) {
        final int t1SeqNum = t1.getSequenceNumber();
        final long t1LocNum = t1.getLocalizationNumber();
        final int t2SeqNum = t2.getSequenceNumber();
        final long t2LocNum = t2.getLocalizationNumber();
        if (t1SeqNum != t2SeqNum) {
            return t1SeqNum - t2SeqNum;
        }
        return (int) (t1LocNum - t2LocNum);
    }

    /** The cause for the update thread being stopped. */
    private enum UpdateStopCause {
        /**
         * Stopped by stopUpdate call, either due to a new topology update
         * (updateDbHandles) or environment being shutdown (closeDbHandles).
         */
        STOPPED,
        /**
         * All update tasks completed successfully.
         */
        SUCCEEDED,
        /**
         * Failed with an exception.
         */
        FAILED,
        /**
         * Some partitions are not ready yet w.r.t. the partition generation.
         */
        NOT_READY,
    }

    private class UpdateThread extends Thread {

        private Topology topology;
        private final ReplicatedEnvironment repEnv;

        private volatile boolean stop = false;

        UpdateThread(Topology topology, ReplicatedEnvironment repEnv) {
            super("KV partition handle updater");
            this.topology = topology;
            this.repEnv = repEnv;
            setDaemon(true);
            setUncaughtExceptionHandler(repNode.getExceptionHandler());
        }

        @Override
        public void run() {

            TestHookExecute.doHookIfSet(
                updateTopoTestHook,
                new UpdateTopoHookObject(repNode, topology));

            try {
                if (!updateStateTracker.maybeStartUpdate(topology)) {
                    return;
                }
                /* Retry as long as there are errors */
                while (true) {
                    final UpdateStopCause cause = update();
                    if (cause == UpdateStopCause.SUCCEEDED) {
                        updateStateTracker.updateComplete(topology);
                        break;
                    }
                    if (cause == UpdateStopCause.STOPPED) {
                        break;
                    }
                    /* Retries for FAILED and NOT_READY. */
                    try {
                        synchronized(this) {
                            wait(DB_UPDATE_RETRY_MS);
                        }
                    } catch (InterruptedException ie) {
                        /* Should not happen. */
                        throw new IllegalStateException(ie);
                    }
                }
            } finally {
                /*
                 * Set null and let JVM to reclaim the memory to avoid OOME.
                 * This thread is referred in JE when open a partition database.
                 * And after the thread exits, the object of the thread is still
                 * referred in JE. The memory retained by the thread object
                 * cannot be reclaimed by JVM. And topology in the thread
                 * object yet cannot be reclaimed by JVM. The topology is a new
                 * object cloned deeply from original one and passed to
                 * UpdataThread before starting the thread. So there will be a
                 * lot of copies of topology in memory when the thread is
                 * started multiple times. And the multiple copies of topology
                 * might cause OOME. And setting topology as null allows JVM to
                 * reclaim the memory to avoid OOME.
                 */
                topology = null;
            }
        }

        /**
         * Updates the partition database handles.
         *
         * @return the stop cause
         */
        private UpdateStopCause update() {
            final PartitionMap partitionMap = topology.getPartitionMap();

            logger.log(Level.FINE,
                       "Establishing partition database handles, " +
                       "topology seq#: {0}",
                       topology.getSequenceNumber());

            final int groupId = repNode.getRepNodeId().getGroupId();
            int errors = 0;
            Throwable latestError = null;
            int rnPartitions = 0;
            boolean allPartitionGenerationsReady = true;

            final PartitionDisplay removed =
                new PartitionDisplay("Removed partition database handles");
            final PartitionDisplay updated =
                new PartitionDisplay("Updated partition database handles");
            final PartitionDisplay notReady =
                new PartitionDisplay("Partition generations not ready");

            final PartitionGenerationTable partitionGenerationTable =
                repNode.getPartGenTable();

            for (Partition p : partitionMap.getAll()) {

                /* Exit if the updater has been stopped, or the env is bad */
                if (stop || !repEnv.isValid()) {
                    logger.log(Level.INFO,
                               "Update terminated, established {0} " +
                               "partition database handles",
                               partitionDbMap.size());
                    /* Will cause thread to exit. */
                    return UpdateStopCause.STOPPED;
                }

                final PartitionId partitionId = p.getResourceId();
                if (p.getRepGroupId().getGroupId() != groupId) {
                    logger.log(Level.FINE,
                               "Removing partition database handle for {0}",
                               partitionId);

                    /* This node does not host the partition. */
                    final Database db = partitionDbMap.remove(partitionId);
                    final MigrationManager mm = repNode.getMigrationManager();
                    mm.removeNoopHandlerWrites(partitionId);

                    /*
                     * If db != null then the partition had moved, so we can
                     * close the database.
                     *
                     * Note that if the partition has migrated the database will
                     * be removed once the topology hs been updated and the
                     * change made "official.
                     * See MigrationManager.localizeTopology().
                     */
                    if (db != null) {
                        logger.log(Level.INFO, "Closing database for moved {0}",
                                   partitionId);
                        removed.add(partitionId);
                        /*
                         * The return can be ignored since the partition
                         * migration transfer is complete and there is nothing
                         * left to do.
                         */
                        final boolean succ = closePartitionDB(db);
                        logger.info("Closed partition database" +
                                    ", pid=" + partitionId +
                                    ", success=" + succ);
                    }
                    continue;
                }
                rnPartitions++;

                try {
                    final boolean refreshed = updatePartitionHandle(partitionId);
                    if (refreshed) {
                        updated.add(partitionId);
                    }
                } catch (RuntimeException re) {
                    if (DatabaseUtils.handleException(
                            re, logger, partitionId.getPartitionName())) {
                        errors++;
                        latestError = re;
                    }
                }

                /*
                 * Check if the generation of this partition is ready (the
                 * generation is open). Note that we must do this check after
                 * we make sure the partition DB is open in
                 * updatePartitionHandle (in case there is an error we do not
                 * count this update as successful anyway, so that is OK). This
                 * is because if there is no migration, the
                 * PartitionGenerationTable#isPartitionOpen method always
                 * return true. Therefore, there could be a race that the
                 * partition generation is not actually open upon return if we
                 * do the following check first.
                 */
                if (!partitionGenerationTable.isPartitionOpen(partitionId)) {
                    allPartitionGenerationsReady = false;
                    notReady.add(partitionId);
                }
            }
            repNode.getRepEnvManager().updateRNPartitions(rnPartitions);

            /*
             * If there have been errors return true (unless the update has been
             * stopped) which will cause the update to be retried.
             */
            if (errors > 0) {
                final Throwable t = latestError;
                logMsg(
                    "update error",
                    () ->
                    String.format(
                        "Partition update error, "
                        + "latest Error: %s, "
                        + "will retry in %s ms",
                        t,
                        DB_UPDATE_RETRY_MS),
                    removed, updated, notReady);
                if (stop) {
                    return UpdateStopCause.STOPPED;
                }
                return UpdateStopCause.FAILED;
            }

            /* Not all partitions ready w.r.t. the generations. */
            if (!allPartitionGenerationsReady) {
                logMsg("Partition generation not ready",
                       () -> "Partition update generations not ready",
                       removed, updated, notReady);
                if (stop) {
                    return UpdateStopCause.STOPPED;
                }
                return UpdateStopCause.NOT_READY;
            }

            /* Success */
            logMsg(() -> "Partition update succeeded",
                   removed, updated, notReady);
            return UpdateStopCause.SUCCEEDED;
        }

        /**
         * Helper class for displaying updates or partition states.
         */
        private class PartitionDisplay {
            private final String name;
            private final List<PartitionId> list = new ArrayList<>();
            private int count = 0;

            private PartitionDisplay(String name) {
                this.name = name;
            }

            private void add(PartitionId pid) {
                count++;
                if (list.size() >= UPDATE_DISPLAY_LIMIT) {
                    return;
                }
                list.add(pid);
            }

            @Override
            public String toString() {
                if (count == 0) {
                    return "";
                }
                return String.format(
                    "%s: %s. ",
                    name,
                    list.stream()
                    .map((p) -> p.toString())
                    .collect(Collectors.joining(
                        ",", "[",
                        (count >= UPDATE_DISPLAY_LIMIT)
                        ? "...]" : "]")));
            }
        }

        private void logMsg(String rateLimitKey,
                            Supplier<String> msg,
                            PartitionDisplay removedToDisplay,
                            PartitionDisplay updatedToDisplay,
                            PartitionDisplay notReadyToDisplay) {
            rateLimitingLogger.log(
                rateLimitKey,
                Level.INFO,
                () ->
                String.format(
                    msg.get() + ". "
                    + "Established %s partition database handles, "
                    + "topology %s. "
                    + "%s%s%s",
                    partitionDbMap.size(),
                    topology.getOrderNumberTuple(),
                    removedToDisplay,
                    updatedToDisplay,
                    notReadyToDisplay));
        }

        private void logMsg(Supplier<String> msg,
                            PartitionDisplay removedToDisplay,
                            PartitionDisplay updatedToDisplay,
                            PartitionDisplay notReadyToDisplay) {
            logger.log(
                Level.INFO,
                () ->
                String.format(
                    msg.get() + ". "
                    + "Established %s partition database handles, "
                    + "topology %s. "
                    + "%s%s%s",
                    partitionDbMap.size(),
                    topology.getOrderNumberTuple(),
                    removedToDisplay,
                    updatedToDisplay,
                    notReadyToDisplay));
        }

        /**
         * Opens the specified partition database and stores its handle in
         * partitionDBMap. Returns {@code true} if actions are taken to refresh
         * the partition database.
         */
        private boolean updatePartitionHandle(final PartitionId partitionId)
            throws ReplicaWriteException {

            /*
             * If there is an existing DB handle for the partition see if it
             * needs updating. There should only be one refresh thread (see
             * updateDbHandles) so there should not be any race condition here.
             */
            final Database currentDB = partitionDbMap.get(partitionId);
            if (!DatabaseUtils.needsRefresh(currentDB, repEnv)) {
                return false;
            }

            /*
             * Use NO_CONSISTENCY so that the handle establishment is not
             * blocked trying to reach consistency particularly when the env is
             * in the unknown state and we want to permit read access.
             */
            final TransactionConfig txnConfig = new TransactionConfig().
               setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);
            txnConfig.setNoWait(true);

            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, txnConfig);
                final Database db =
                    repEnv.openDatabase(txn, partitionId.getPartitionName(),
                                        partitionDbConfig);

                txn.commit();
                txn = null;
                /* DB op successful, update in-memory data */
                partitionDbMap.put(partitionId, db);
                return true;
            } catch (IllegalStateException e) {

                /*
                 * The exception was most likely thrown because the environment
                 * was closed.  If it was thrown for another reason, though,
                 * then invalidate the environment so that the caller will
                 * attempt to recover by reopening it.
                 */
                if (repEnv.isValid()) {
                    EnvironmentFailureException.unexpectedException(
                        DbInternal.getEnvironmentImpl(repEnv), e);
                }
                throw e;

            } finally {
               TxnUtil.abort(txn);
            }
        }

        /**
         * Stops the updater and waits for the thread to exit.
         */
        void waitForStop() {
            assert Thread.currentThread() != this;

            stop = true;
            synchronized(this) {
                notifyAll();
            }

            try {
                join();
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }
}
