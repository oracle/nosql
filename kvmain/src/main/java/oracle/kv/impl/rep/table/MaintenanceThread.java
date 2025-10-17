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

package oracle.kv.impl.rep.table;

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_DEFAULT;
import static oracle.kv.impl.rep.PartitionManager.DB_UPDATE_RETRY_MS;
import static oracle.kv.impl.rep.RNTaskCoordinator.KV_INDEX_CREATION_TASK;
import static oracle.kv.impl.rep.table.SecondaryInfoMap.CLEANER_CONFIG;
import static oracle.kv.impl.rep.table.SecondaryInfoMap.SECONDARY_INFO_CONFIG;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Scanner;
import oracle.kv.impl.api.ops.TableIterate;
import oracle.kv.impl.api.ops.TableIterateHandler;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.ShutdownThread;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Table;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.utilint.TaskCoordinator;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;

/**
 * Thread to manage table maintenance. This thread is run on RN startup
 * and any time the table metadata is updated. When run, the table
 * metadata is scanned looking for indexes. Each index will have a
 * corresponding secondary DB. If the index is new, (and this is the master)
 * a new secondary DB is created.
 *
 * The table MD is also checked for changes, such as indexes dropped and
 * tables that need their data deleted.
 *
 * Once the scan is complete, any new or pending maintenance is done.
 * Maintenance only runs on the master.
 *
 * Note that secondary cleaning activity and partition migration cannot occur
 * concurrently, see [#24245].
 *
 * A task was added to convert table metadata records serial format
 * from FastExterlizable to Java serialization [KVSTORE-1394]. This task
 * will run to completion the first time the maintenance thread is run
 * after the RN becomes a master. The task can be removed once the
 * conversion is not longer needed (see PREREQUISITE_VERSION check in
 * TableMetadataPersistence).
 */
public class MaintenanceThread extends ShutdownThread {

    private static final long ONE_SECOND_MS = 1000L;

    /* DB operation delays, in ms */
    private static final long SHORT_RETRY_WAIT_MS = 500L;
    private static final long LONG_RETRY_WAIT_MS = 1000L;
    private static final long VERY_LONG_RETRY_WAIT_MS = 30000L;

    /* Number of records read from the primary during each populate call. */
    public static final int POPULATE_BATCH_SIZE = 500;

    /*
     * Number of records read from a secondary DB partition in a transaction
     * when cleaning after a partition migration.
     */
    private static final int CLEAN_BATCH_SIZE = 100;

    final TableManager tableManager;

    private final RepNode repNode;

    /* Used to coordinate table maint tasks with other housekeeping tasks */
    private final TaskCoordinator taskCoordinator;

    private final Logger logger;

    private ReplicatedEnvironment repEnv = null;

    private int lastSeqNum = Metadata.UNKNOWN_SEQ_NUM;

    /**
     * True if there has been a request to abandon the current maintenance
     * operation, and perform a new update and associated maintenance.
     * Synchronize when setting this field.
     */
    private volatile boolean updateRequested = false;

    /**
     * The previous maintenance thread, if there was one. This is so that the
     * new thread can wait for the old thread to exit. This avoids requiring
     * callers who start the thread to wait.
     */
    private MaintenanceThread oldThread;

    MaintenanceThread(MaintenanceThread oldThread,
                      TableManager tableManager,
                      RepNode repNode,
                      Logger logger) {
        super(null, repNode.getExceptionHandler(), "KV table maintenance");
        this.oldThread = oldThread;
        this.tableManager = tableManager;
        this.repNode = repNode;
        this.taskCoordinator = repNode.getTaskCoordinator();
        this.logger = logger;
    }

    @Override
    public void run() {
        if (isStopped()) {
            return;
        }

        /* Make sure the old thread is stopped */
        if (oldThread != null) {
            oldThread.shutdown();
            oldThread = null;
        }
        logger.log(Level.INFO, "Starting {0}", this);

        while (!isStopped()) {
            /*
             * A fixed is needed here. There are calls within the catch blocks
             * that can throw additional exceptions, likely during shutdown.
             * This try-catch needs to be split between catching exceptions
             * that we can continue from (DBEs, lock conflicts) and exceptions
             * that we can't (IEs, REs...).
             */
            try {
                repEnv = repNode.getEnv(0);
                if (repEnv == null) {
                    retryWait(LONG_RETRY_WAIT_MS);
                    continue;
                }

                final TableMetadata tableMd = tableManager.getTableMetadata();
                if (tableMd == null) {
                    retryWait(LONG_RETRY_WAIT_MS);
                    continue;
                }

                /*
                 * If the table metadata needs serialization update, do
                 * that first so that it is completed as quickly as possible.
                 */
                if (repEnv.getState().isMaster()) {
                    tableManager.doSerializationUpdate();
                }

                /*
                 * If update() returns true, all maintenance for the given
                 * metadata is complete. In that case, check whether we are
                 * really done . If so, exit, otherwise do another update.
                 */
                if (update(tableMd) && checkForDone()) {
                    return;
                }
            } catch (DatabaseNotFoundException |
                     ReplicaWriteException |
                     UnknownMasterException |
                     InsufficientAcksException |
                     InsufficientReplicasException |
                     DiskLimitException de) {
                /* If shutdown, simply exit */
                if (isStopped()) {
                    return;
                }

                /*
                 * DatabaseNotFoundException and ReplicaWriteException
                 * are likely due to the replica trying to update before
                 * the master has done an initial update.
                 */
                if (!repNode.hasAvailableLogSize()) {
                    /*
                     * Use a longer period when we reaches disk limit exception
                     * (e.g., when InsufficientReplicasException or
                     * DiskLimitException is thrown).
                     */
                    retryWait(VERY_LONG_RETRY_WAIT_MS);
                } else {
                    retryWait(LONG_RETRY_WAIT_MS);
                }
            } catch (LockConflictException lce) {
                retryWait(SHORT_RETRY_WAIT_MS);
            } catch (InterruptedException ie) {
                /* IE can happen during shutdown */
                if (isStopped()) {
                    logger.log(Level.INFO, "{0} exiting after, {1}",
                               new Object[]{this, ie});
                    return;
                }
                throw new IllegalStateException(ie);
            } catch (RuntimeException re) {
                /*
                 * If shutdown or env is bad just exit.
                 */
                if (isStopped()) {
                    logger.log(Level.INFO, "{0} exiting after, {1}",
                               new Object[]{this, re});
                    return;
                }
                throw re;
            }
        }
    }

    /**
     * Waits for a period of time. Returns if the specified amount of time
     * has passed, or the thread is stopped.
     *
     * @param waitMS the wait time, a negative value means return immediately
     */
    public void retryWait(long waitMS) {
        if (waitMS <= 0) {
           return;
        }

        try {
            waitForMS(waitMS);
        } catch (InterruptedException ie) {
            /* IE may happen during shutdown */
            if (!isStopped()) {
                getLogger().log(Level.SEVERE,
                                "Unexpected exception in {0}: {1}",
                                new Object[]{this, ie});
                throw new IllegalStateException(ie);
            }
        }
    }

    /*
     * Waits for a period of time. Returns if the specified amount of time
     * has passed, or updateRequested is true, or the thread is stopped. This
     * wait should be used only when waiting in update(), otherwise may cause
     * busy spin (e.g., in the main loop[KVSTORE-925]).
     *
     * @param waitMS the wait time, a negative value means return immediately
     */
    private synchronized void waitInUpdate(long waitMS) {
        if (!exitMaintenance()) {
            retryWait(waitMS);
        }
    }

    /**
     * Returns true if there is no more work or the thread is shutdown.
     */
    private synchronized boolean checkForDone() {
        if (updateRequested) {
            /* There has been an update request, clear it and return */
            updateRequested = false;
            return isShutdown();
        }
        /* No update request, we are done */
        shutdown();
        return true;
    }

    /**
     * Notifies the maintenance thread that there is an update to the table
     * metadata. Returns true if the request was successful. If successful
     * the current thread will abandon it's current maintenance operation,
     * get the latest metadata, and restart maintenance.
     */
    boolean requestUpdate() {
        return requestUpdate(false);
    }

    private synchronized boolean requestUpdate(boolean force) {
        if (isShutdown()) {
            return false;
        }
        updateRequested = true;

        if (force) {
            /*
             * Setting lastSeqNum to UNKNOWN_SEQ_NUM will force a call to
             * updateInternal during the update.
             */
            lastSeqNum = Metadata.UNKNOWN_SEQ_NUM;
        }
        notifyWaiters();
        return true;
    }

    /* -- From ShutdownThread -- */

    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Updates the SecondaryInfoMap with dropped tables and added indexes
     * that require action via maintenance threads to populate indexes or
     * clean out deleted tables.
     *
     * @return true if the update was successful and maintenance complete
     */
    private boolean update(TableMetadata tableMd)
            throws InterruptedException {
        if (isStopped()) {
            return false;
        }
        logger.log(Level.INFO,
                   "Establishing secondary database handles, " +
                   "table seq#: {0}",
                   tableMd.getSequenceNumber());
        try {
            final Database infoDb = tableManager.getInfoDb(repEnv);

            /*
             * Update maps if the local state needs updating, if any
             * secondaries have been reset, or this is new table metadata.
             */
            boolean update = tableManager.checkLocalState(repEnv, true);
            if (resetSecondaries(infoDb)) {
                update = true;
            }
            if (update || tableMd.getSequenceNumber() > lastSeqNum) {
                /* Return false if update failed */
                if (!updateInternal(tableMd, infoDb)) {
                    return false;
                }
                lastSeqNum = tableMd.getSequenceNumber();
            }
            doMaintenance(infoDb);
            return true;
        } finally {
            assert !tableManager.isBusyMaintenance();
        }
    }

    /**
     * Resets any secondaries which have been invalidated due to corruption.
     * Returns true if any secondary was successfully reset.
     */
    private boolean resetSecondaries(Database infoDb) {
        final Map<String, SecondaryIntegrityException> invalidatedSecondaries =
                                    tableManager.getInvalidatedSecondaries();
        if (invalidatedSecondaries == null) {
            return false;
        }

        boolean needsUpdate = false;
        for (SecondaryIntegrityException sie : invalidatedSecondaries.values()){
            if (tableManager.resetSecondary(infoDb, sie, null /*already logged*/,
                                            repEnv)) {
                needsUpdate = true;
            }
        }
        /*
         * At this point the invalidated DBs will be removed, preventing
         * further operations to the secondary, so we can clear out the map.
         * Since the map is not synchronized, it is possible that a new entry
         * was added after the above iteration, but that is OK because there
         * will very likely be another operation which will invalidate the
         * secondary.
         */
        invalidatedSecondaries.clear();
        return needsUpdate;
    }

    private boolean updateInternal(TableMetadata tableMd, Database infoDb) {
        /*
         * Map of secondary DB name -> indexes that are defined in the table
         * metadata. This is used to determine what databases to open and if
         * an index has been dropped.
         */
        final Map<String, IndexImpl> indexes = new HashMap<>();

        /*
         * Set of tables which are being removed but first need their data
         * deleted.
         */
        final Set<TableImpl> deletedTables = new HashSet<>();

        for (Table table : tableMd.getTables().values()) {
            TableManager.scanTable((TableImpl)table, indexes, deletedTables);
        }

        if (logger.isLoggable(Level.FINE)) {
            /* Enumerate the indexes and tables for logging purposes */
            final StringBuilder indexesString = new StringBuilder();
            final StringBuilder tablesString = new StringBuilder();
            for (String indexName : indexes.keySet()) {
                if (indexesString.length() > 0) {
                    indexesString.append(" ");
                }
                indexesString.append(indexName);
            }
            for (TableImpl table : deletedTables) {
                if (tablesString.length() > 0) {
                    tablesString.append(" ");
                }
                tablesString.append(table.getFullNamespaceName());
            }
            logger.log(Level.FINE,
                       "Found {0} indexes({1}) and {2} tables" +
                       " marked for deletion({3}) in {4})",
                       new Object[]{indexes.size(),
                                    indexesString.toString(),
                                    deletedTables.size(),
                                    tablesString.toString(),
                                    tableMd});
        } else if (!deletedTables.isEmpty()) {
            logger.log(Level.INFO, "Found {0} tables" +
                       " marked for deletion in {1})",
                       new Object[]{deletedTables.size(), tableMd});
        }

        /*
         * Update the secondary map with any indexes that have been dropped
         * and removed tables. Changes to the secondary map can only be
         * made by the master. This call will also remove the database.
         * This call will not affect secondaryDbMap which is dealt with
         * below.
         */
        if (repEnv.getState().isMaster()) {
            SecondaryInfoMap.update(tableMd, indexes, deletedTables,
                                    this,  infoDb, repEnv, logger);
        }

        final Set<String> secondaryDbs = tableManager.getSecondaryDbs();

        /*
         * Remove entries from secondaryDbMap for dropped indexes.
         * The call to SecondaryInfoMap.check above will close the DB
         * when the master, so this will close the DB on the replica.
         */
        final Iterator<String> itr = secondaryDbs.iterator();

        while (itr.hasNext()) {
            if (isStopped()) {
                return false;
            }
            final String dbName = itr.next();

            if (!indexes.containsKey(dbName)) {
                if (tableManager.closeSecondary(dbName)) {
                    itr.remove();
                }
            }
        }

        /*
         * For each index open its secondary DB
         */
        final int errors = tableManager.openSecondaryDBs(indexes, infoDb,
                                                         repEnv, this);
        if (isStopped()) {
            logger.log(Level.INFO,
                       "Update terminated, established {0} " +
                       "secondary database handles",
                       secondaryDbs.size());
            return false;
        }

        /*
         * If there have been errors return false which will cause the
         * update to be retried after a delay.
         */
        if (errors > 0) {
            logger.log(Level.INFO,
                       "Established {0} secondary database handles, " +
                       "will retry in {1}ms",
                       new Object[] {secondaryDbs.size(),
                                     DB_UPDATE_RETRY_MS});
            retryWait(DB_UPDATE_RETRY_MS);
            return false;
        }
        logger.log(Level.INFO, "Established {0} secondary database handles",
                   secondaryDbs.size());

        return true;   // Success
    }

    /**
     * Closes the specified secondary database. Returns true if the close
     * succeeded, or the database was not open.
     *
     * @param dbName the name of the secondary DB to close
     * @return true on success
     */
    boolean closeSecondary(String dbName) {
        return tableManager.closeSecondary(dbName);
    }

    /**
     * Performs maintenance operations as needed. Does not return until all
     * maintenance is complete, the thread is shutdown, or there has been
     * a request to update.
     */
    private void doMaintenance(Database infoDb) throws InterruptedException {
        if (!repEnv.getState().isMaster()) {
            return;
        }

        SecondaryInfoMap secondaryInfoMap = SecondaryInfoMap.fetch(infoDb);

        /*
         * On each pass, check whether any of the maintenance operations are
         * needed. Each operation will check if there is other work to do, and
         * if so will exit after doing a chunk of work. This way everyone makes
         * progress. The operation methods will return an updated secondary info
         * map.
         */
        while (!exitMaintenance()) {
            boolean done = true;

            if (secondaryInfoMap.secondaryNeedsPopulate()) {
                secondaryInfoMap = populateSecondary(secondaryInfoMap, infoDb);
                done = false;
            }
            if (secondaryInfoMap.tableNeedCleaning()) {
                secondaryInfoMap = cleanPrimary(secondaryInfoMap, infoDb);
                done = false;
            }
            if (secondaryInfoMap.secondaryNeedsCleaning()) {
                secondaryInfoMap = cleanSecondary(secondaryInfoMap, infoDb);
                done = false;
            }
            if (done) {
                /* Nothing to do */
                return;
            }
        }
        /*
         * Exiting because exitMaintenance return true. This means the
         * thread is exiting, or we need to return to update.
         */
    }

    /**
     * Returns true if maintenance operations should stop, or the thread
     * is shutdown.
     */
    public boolean exitMaintenance() {
        return updateRequested || isStopped();
    }

    /**
     * Returns true if the thread is shutdown or the environment is invalid.
     */
    boolean isStopped() {
        return isShutdown() || ((repEnv != null) && !repEnv.isValid());
    }

    /**
     * Populates the secondary databases. Secondary DBs (indexes) are populated
     * by scanning through each partition DB. During the scan the secondary
     * DB has Incremental Population set to true to prevent
     * SequenceIntegrityExceptions from being thrown.
     *
     * The population of indexes is done in chunks as it round robins across
     * indexes. A chunk could be as much as a partition in size, or it could be
     * less than a partition if the throughput limit of the table might be
     * exceeded before all the records for the table in the partition was
     * processed.
     *
     * The method will exit if it finds that there is other pending maintenance
     * work (table or secondary cleaning) between each chunk of index
     * population work, so that all maintenance work gets a chance to run in
     * this single maintenance thread as a form of cooperative multi-tasking.
     *
     * @return the SecondaryInfoMap updated to reflect any progress in
     * populating secondaries
     *
     * @throws InterruptedException if the wait for a permit is interrupted
     */
    private SecondaryInfoMap populateSecondary(SecondaryInfoMap infoMap,
                                               Database infoDb)
            throws InterruptedException {
        logger.info("Running secondary population");

        /* If there is other pending work, just make one pass */
        final boolean workPending = infoMap.tableNeedCleaning() ||
                                    infoMap.secondaryNeedsCleaning();

        final OperationHandler oh =
            new OperationHandler(repNode, tableManager.getParams());
        final TableIterateHandlerInternal opHandler =
            new TableIterateHandlerInternal(oh);
        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        final long permitTimeoutMs =
            repNodeParams.getPermitTimeoutMs(KV_INDEX_CREATION_TASK);
        final long permitLeaseMs =
            repNodeParams.getPermitLeaseMs(KV_INDEX_CREATION_TASK);

        /*
         * Since we are not using the timeout mechanism in the task coordinator
         * we keep track of the timeout for getting a permit externally.
         */
        final long startMs = System.currentTimeMillis();

        /*
         * We populate secondaries by scanning through one partition at a time.
         * This can be done without interlocking with partition migration
         * until it is time to determine if scanning is done. To know whether
         * we are done, migration target processing needs to be idle so that the
         * list of partitions is stable. The waitForTargetIdle flag indicates
         * if migration needs to be idle and is set when it appears that we
         * are done.
         */
        boolean waitForTargetIdle = false;

        /*
         * Map from dbName -> ResourceCollector. This will only contain
         * entries for indexes who's tables have throughput limits.
         */
        final Map<String, ResourceCollector> throughputCollectorMap =
                                                                new HashMap<>();

        do {
            Transaction txn = null;
            /*
             * Attempt to get a work permit without a timeout. If it fails
             * (isDeficit == true) we wait. By waiting we will exit if there
             * is a update.
             */
            try (final Permit permit = taskCoordinator.
                acquirePermit(KV_INDEX_CREATION_TASK,
                              0 /* timeout */,
                              permitLeaseMs,
                              TimeUnit.MILLISECONDS)) {

                final long timeStamp = System.currentTimeMillis();

                /*
                 * If the acquire failed wait a short bit to retry unless we
                 * are past the permit timeout, in which case proceed with the
                 * population.
                 */
                if (permit.isDeficit() &&
                    (timeStamp - startMs) < permitTimeoutMs) {
                    permit.releasePermit();

                    /* Exit if there is other work to do */
                    if (workPending) {
                        break;
                    }
                    waitInUpdate(SHORT_RETRY_WAIT_MS);
                    continue; /* loop back to while (!exitMaintenance()) */
                }

                /*
                 * For all but the last pass through the loop waitForTargetIdle
                 * will be false, letting population proceed without waiting
                 * for migration to idle.
                 */
                if (waitForTargetIdle) {
                    tableManager.setBusyMaintenance();
                    repNode.getMigrationManager().awaitTargetIdle(this);
                    /*
                     * Since we are about to finish, don't check for pending
                     * work to exit.
                     */
                    if (exitMaintenance()) {
                        break;
                    }
                    waitForTargetIdle = false;
                }
                txn = repEnv.beginTransaction(null, SECONDARY_INFO_CONFIG);

                infoMap = SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                /*
                 * The iterate order of the set returned by
                 * getSecondariesToPopulate will be from least recently
                 * populated to most recent.
                 */
                final Set<Entry<String, SecondaryInfo>> entries =
                                            infoMap.getSecondariesToPopulate();

                /* If no more, we are finally done */
                if (entries.isEmpty()) {
                    logger.info("Completed populating secondary " +
                                "database(s)");
                    break;
                }

                String secondaryDbName = null;
                SecondaryInfo info = null;
                long oldestPass = 0L;

                /*
                 * Make a pass through the indexes that need to be populated
                 * looking for one which can be populated right now. The loop
                 * will exit with dbName and info set to the index to be
                 * populated, or dbName == null if no indexes are ready to
                 * populate. If dbName == null, oldestPass will be set to the
                 * longest time that a population has been waiting.
                 */
                for (Entry<String, SecondaryInfo> entry : entries) {
                    secondaryDbName = entry.getKey();
                    info = entry.getValue();
                    assert info.needsPopulating();

                    /*
                     * If not throttled (or first time here), populate this
                     * secondary.
                     */
                    final ResourceCollector rc =
                        throughputCollectorMap.get(secondaryDbName);
                    if ((rc == null) ||
                        !rc.isThrottled(false /*checkAccess*/, null)) {
                        break;
                    }

                    /*
                     * If here, the secondary could not be populated. Record the
                     * oldest so we know how long to wait if needed.
                     */
                    final long lastPass = info.msSinceLastPass(timeStamp);
                    if (lastPass > oldestPass) {
                        oldestPass = lastPass;
                    }
                    secondaryDbName = null;
                }
                /*
                 * This check is unnecessary and is here to keep the code
                 * checker happy.
                 */
                if (info == null) {
                    throw new AssertionError();
                }

                /*
                 * If secondaryDbName is null, there are no secondaries ready
                 * to populate (all are being throttled). Briefly wait and
                 * retry.
                 */
                if (secondaryDbName == null) {
                    TxnUtil.abort(txn);
                    txn = null;
                    permit.releasePermit();

                    /* Exit if there is other work to do */
                    if (workPending) {
                        break;
                    }
                    /*
                     * The wait time is how long to wait so that the oldest
                     * population is at least a second old, pushing it into
                     * the next throttle period.
                     */
                    final long waitTime = ONE_SECOND_MS - oldestPass;
                    waitInUpdate(waitTime);
                    continue;
                }
                assert secondaryDbName != null;
                assert info != null;

                logger.log(Level.FINE, "Started populating {0}",
                           secondaryDbName);

                /*
                 * If there is no current partition to process, set a new
                 * one. setCurrentPartition() will return false if we should
                 * retry with migration idle. In that case set waitForTargetIdle
                 * and make another pass.
                 */
                if ((info.getCurrentPartition() == null) &&
                    !setCurrentPartition(info)) {
                    waitForTargetIdle = true;
                    continue;
                }

                final SecondaryDatabase secondaryDb =
                    tableManager.getSecondaryDb(secondaryDbName);

                if (info.getCurrentPartition() == null) {
                    logger.log(Level.FINE, "Finished populating {0} {1}",
                               new Object[]{secondaryDbName, info});
                    throughputCollectorMap.remove(secondaryDbName);

                    assert secondaryDb != null;
                    tableManager.endPopulation(secondaryDbName);

                    info.donePopulation();
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                    continue;
                }

                /*
                 * secondaryDb can be null if the update thread has not
                 * yet opened all of the secondary databases.
                 */
                if (secondaryDb == null) {
                    /*
                     * By updating the last pass time this table will be
                     * pushed to the back of the list. If there is only
                     * one index to populate, wait a short time to allow
                     * update to finish.
                     */
                    info.completePass(timeStamp);
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                    if (entries.size() == 1) {
                        permit.releasePermit();

                        /* Exit if there is other work to do */
                        if (workPending) {
                            break;
                        }
                        final long waitTime = ONE_SECOND_MS - oldestPass;
                        waitInUpdate(waitTime);
                    }
                    continue;
                }
                logger.log(Level.FINE, "Populating {0} {1}",
                           new Object[]{secondaryDbName, info});
                assert TestHookExecute.doHookIfSet(tableManager.populateHook,
                                                   secondaryDb);
                final String tableName =
                    TableManager.getTableName(secondaryDb.getDatabaseName());
                final String namespace =
                    TableManager.getNamespace(secondaryDb.getDatabaseName());
                final TableImpl table = tableManager.getTable(namespace,
                                                              tableName);

                if (table == null) {
                    final String nsName =
                        NameUtils.makeQualifiedName(namespace, tableName);
                    logger.log(Level.WARNING,
                               "Failed to populate {0}, missing table {1}",
                               new Object[]{info, nsName});
                    break;
                }

                try {
                    final ResourceCollector rc =
                             tableManager.getResourceCollector(table.getId());
                    /*
                     * If being throttled, keep the collector so that we can
                     * check whether we need to skip due to being throttled.
                     */
                    if (rc != null) {
                        throughputCollectorMap.put(secondaryDbName, rc);
                    }

                    final boolean more = populate(table,
                                                  opHandler,
                                                  txn,
                                                  info,
                                                  rc);
                    info.completePass(timeStamp);
                    if (!more) {
                        logger.log(Level.FINE, "Finished partition for {0}",
                                   info);
                        info.completeCurrentPartition();
                    }
                    infoMap.persist(infoDb, txn);
                } catch (SecondaryIntegrityException sie) {
                    /*
                     * If there is an SIE the secondary DB is corrupt. We will
                     * restart population using a fresh DB. resetSecondary
                     * will remove the current DB and reset the population info.
                     * The txn is no longer valid.
                     */
                    TxnUtil.abort(txn);
                    txn = null;

                    if (tableManager.resetSecondary(infoDb, sie,
                                                    "secondary population",
                                                    repEnv)) {
                        /*
                         * Calling requestUpdate(true) will cause the
                         * populateSecondary loop to exit and will force a call
                         * to update, which will then open a new DB and restart
                         * population.
                         */
                        requestUpdate(true);
                    }
                    break;
                } catch (RNUnavailableException rue) {
                    /*
                     * This can happen if the metadata is updated during
                     * a population. In that case a new DB name can be added
                     * to secondaryLookupMap before the DB is opened and
                     * inserted into secondaryDbMap. Even though this is
                     * populating secondary A, the populate() call will
                     * result in a callback to getSecondaries().
                     * getSecondaries() requires all DBs named in
                     * secondaryLookupMap be open and present in
                     * secondaryDbMap.
                     */
                    logger.log(Level.INFO, "Index population failed " +
                               "on {0}: {1} populate will be retried",
                               new Object[]{secondaryDbName, rue.getMessage()});
                    break;
                } catch (IncorrectRoutingException ire) {
                    /*
                     * An IncorrectRoutingException is thrown if the
                     * partition has moved and its DB removed. In this
                     * case we can mark it complete and move on. The txn
                     * is still valid.
                     */
                    handleIncorrectRoutingException(infoMap, info, infoDb, txn);
                } catch (IllegalStateException ise) {
                    /*
                     * In addition to IncorrectRoutingException, an
                     * IllegalStateException can be thrown if the partition is
                     * moved during population (and its DB closed). The call to
                     * getPartitionDB() will throw IncorrectRoutingException
                     * if that is the case. Since the ise was likely thrown
                     * from JE, the txn is probably not valid.
                     */
                    TxnUtil.abort(txn);
                    txn = null;

                    try {
                        repNode.getPartitionDB(info.getCurrentPartition());
                    } catch (IncorrectRoutingException ire) {
                        /*
                         * Mark this partition as done, commit that fact
                         * and continue (don't exit).
                        */
                        handleIncorrectRoutingException(secondaryDbName, infoDb);
                        continue;
                    }

                    persistInfoErrorString(secondaryDbName, infoDb, ise);
                    /* return normally to move on */
                    break;

                } catch (RuntimeException re) {
                    /* Includes ResourceLimitException */
                    TxnUtil.abort(txn);
                    txn = null;

                    persistInfoErrorString(secondaryDbName, infoDb, re);
                    /* return normally to move on */
                    break;
                }
                txn.commit();
                txn = null;
            } finally {
                TxnUtil.abort(txn);
                /* Busy may have been set */
                tableManager.clearBusyMaintenance();
            }
        } while (!exitMaintenance() && !workPending);

        return infoMap;
    }

    /**
     * Sets the current partition field in the specified info object. The
     * current partition is the partition that requires maintenance. If
     * no partition is found, the field is not modified. Returns true on
     * success. If false is returned, the call should be retried with
     * migration idle.
     *
     * @return true if the current partition was set
     */
    private boolean setCurrentPartition(SecondaryInfo info) {
        assert info.needsPopulating();
        assert info.getCurrentPartition() == null;

        /* Loop to find a partition that needs processing. */
        for (PartitionId partition : repNode.getPartitions()) {
            if (!info.isCompleted(partition)) {
                info.setCurrentPartition(partition);
                return true;
            }
        }

        /*
         * No partition was found, so it appears that we are done. However,
         * if isBusyMaintenance() == false the list of partitions on this RN
         * may have changed due to migration. In that case we return false to
         * indicate we should check again while migration target processing
         * is idle.
         */
        return tableManager.isBusyMaintenance();
    }

    private void handleIncorrectRoutingException(String dbName,
                                                 Database infoDb) {
        Transaction txn = null;
        try {
            txn = repEnv.beginTransaction(null, SECONDARY_INFO_CONFIG);
            final SecondaryInfoMap infoMap =
                            SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

            /* Reset secondary info */
            final SecondaryInfo info = infoMap.getSecondaryInfo(dbName);
            handleIncorrectRoutingException(infoMap, info, infoDb, txn);
            txn.commit();
            txn = null;
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Index population failed on {0} due to partition" +
                       " being moved, unable to persist info due to: {1}",
                       new Object[]{dbName, e.getMessage()});
        } finally {
            TxnUtil.abort(txn);
        }
    }

    /**
     * Handle an IncorrectRoutingException caught during secondary population.
     * An IncorrectRoutingException is thrown if the partition has moved and
     * its DB removed. In this case we can mark it complete.
     */
    private void handleIncorrectRoutingException(SecondaryInfoMap infoMap,
                                                 SecondaryInfo info,
                                                 Database infoDb,
                                                 Transaction txn) {
        logger.log(Level.FINE, "Finished partition for {0}, " +
                   "partition no longer in this shard", info);
        info.completeCurrentPartition();
        infoMap.persist(infoDb, txn);
    }

    /**
     * Persists the error string for the specified database. This will stop
     * population to that DB.
     */
    private void persistInfoErrorString(String dbName,
                                        Database infoDb,
                                        Exception exception) {
        Transaction txn = null;
        try {
            txn = repEnv.beginTransaction(null, SECONDARY_INFO_CONFIG);
            final SecondaryInfoMap infoMap =
                            SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);
            final SecondaryInfo info = infoMap.getSecondaryInfo(dbName);
            if (info != null) {
                info.setErrorString(exception);
                infoMap.persist(infoDb, txn);
                txn.commit();
                txn = null;
                logger.log(Level.WARNING,
                           "Index population failed on {0}: {1}",
                           new Object[]{dbName, info});
            } else {
                logger.log(Level.WARNING,
                           "Index population failed on {0}: due to {1}" +
                           ", unable to persist error string due info" +
                           " object missing",
                           new Object[]{dbName, exception.getMessage()});
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                       "Index population failed, on {0} due to: {1}" +
                       ", unable to persist error string due to: {2}",
                       new Object[]{dbName, exception.getMessage(),
                                    e.getMessage()});
        } finally {
            TxnUtil.abort(txn);
        }
    }

    /**
     * Processes records for the specified table from the current partition.
     * Returns true if there are more records to read from the
     * partition. If true is returned the bytes in info.lastKey are set to the
     * key of the last record read.
     *
     * @param table the source table
     * @param opHandler the internal operation handler
     * @param txn current transaction
     * @return true if there are more records to process
     * @throws InterruptedException if the wait for a Permit is interrupted
     */
    private boolean populate(TableImpl table,
                             TableIterateHandlerInternal opHandler,
                             Transaction txn,
                             SecondaryInfo info,
                             ResourceCollector rc) {
        /*
         * If the bytes in lastKey is not null then use that to start the
         * iteration. Set to the last key read on exit.
         */
        final byte[] resumeKey = info.getLastKey().getData();

        final PrimaryKey pkey = table.createPrimaryKey();
        final TableKey tkey = TableKey.createKey(table, pkey, true);

        /* A single target table */
        final TargetTables targetTables = new TargetTables(table, null, null);

        /* Create and execute the iteration */
        final TableIterate op = new TableIterate(tkey.getKeyBytes(),
                                                 targetTables,
                                                 tkey.getMajorKeyComplete(),
                                                 POPULATE_BATCH_SIZE,
                                                 resumeKey,
                                                 false/* inclTombstones */);

        if ((rc == null) || info.isRebuild()) {
            /*
             * If there are no limits or this is a rebuild we still want to
             * record activity
             */
            op.setResourceTracker(repNode.getAggregateThroughputTracker(),
                                  Consistency.NONE_REQUIRED);
        } else {
            /*
             * Check to make sure we can write and that the throughout limit
             * has not been exceeded. Note that this may result in starvation
             * but it is up to the client to control their application when
             * creating indexes. If access is denied, an exception is thrown
             * and this population will fail.
             */
            if (rc.isThrottled(true, info.getCurrentPartition())) {
                return true;
            }
            op.setResourceTracker(rc, Consistency.NONE_REQUIRED);
        }
        return opHandler.populate(op, txn, info);
    }

    /*
     * Special internal handler which bypasses security checks.
     */
    private static class TableIterateHandlerInternal
            extends TableIterateHandler {
        TableIterateHandlerInternal(OperationHandler handler) {
            super(handler);
        }

        /*
         * Overriding verifyTableAccess() will bypass keyspace and
         * security checks.
         */
        @Override
        public void verifyTableAccess(TableIterate op) { }

        private boolean populate(TableIterate op,
                                 Transaction txn,
                                 SecondaryInfo info) {

            /*
             * Use READ_UNCOMMITTED_ALL and keyOnly to make it inexpensive to
             * skip records that don't match the table. Once they match, lock
             * them, and use them.
             *
             * TODO: can this loop be combined with the loop in TableIterate
             * to share code?
             */
            final OperationTableInfo tableInfo = new OperationTableInfo();
            final Scanner scanner = getScanner(op,
                                               tableInfo,
                                               txn,
                                               info.getCurrentPartition(),
                                               CURSOR_DEFAULT,
                                               LockMode.READ_UNCOMMITTED_ALL,
                                               true); // key-only
            boolean moreElements = false;

            try {
                final DatabaseEntry lastKey = info.getLastKey();
                final DatabaseEntry keyEntry = scanner.getKey();

                /* this is used to do a full fetch of data when needed */
                final DatabaseEntry dentry = new DatabaseEntry();
                int numElements = 0;

                /*
                 * Called with noChargeEmpty == true to avoid a read charge
                 * on empty partition.
                 * TODO - can we avoid charges if the batch ended on the last
                 * record, making this return false?
                 */
                while ((moreElements = scanner.next(true)) == true) {

                    final int match = keyInTargetTable(op,
                                                       tableInfo,
                                                       scanner,
                                                       true /*chargeReadCost*/);
                    if (match > 0) {

                        /*
                         * The iteration was done using READ_UNCOMMITTED_ALL
                         * and with the cursor set to getPartial().  It is
                         * necessary to call getLockedData() here to both lock
                         * the record and fetch the data. populateSecondaries()
                         * must be called with a locked record.
                         */
                        if (scanner.getLockedData(dentry, false, true)) {

                            if (!TableImpl.isTableData(dentry.getData(), null)){
                                continue;
                            }

                            final byte[] keyBytes = keyEntry.getData();
                            /* Sets the lastKey return */
                            lastKey.setData(keyBytes);
                            scanner.getDatabase().
                            populateSecondaries(txn, lastKey, dentry,
                                                scanner.getCreationTime(),
                                                scanner.getModificationTime(),
                                                scanner.getExpirationTime(),
                                                scanner.getCurrentStorageSize(),
                                                scanner.isTombstone(),
                                                null);
                            /* Charge for the secondary DB write */
                            op.addWriteBytes(0, 1, null, 0);
                            ++numElements;
                        }
                    } else if (match < 0) {
                        moreElements = false;
                        break;
                    }
                    if (numElements >= POPULATE_BATCH_SIZE) {
                        break;
                    }
                    info.accumulateThroughput(op.getReadKB(), op.getWriteKB());
                }
            } finally {
                scanner.close();
            }
            return moreElements;
        }
    }

    /**
     * Cleans secondary databases. A secondary needs to be "cleaned"
     * when a partition has moved from this node. In this case, secondary
     * records that are from primary records in the moved partition need to be
     * removed the secondary. Cleaning is done by reading each record in a
     * secondary DB and checking whether the primary key is from a missing
     * partition. If so, remove the secondary record. Cleaning needs to happen
     * on every secondary whenever a partition has migrated.
     *
     * Secondary cleaning cannot take place on the source when there is a
     * completed transfer and the migration is waiting to complete. This is
     * because the migration may still fail and the partition re-instated on
     * the source. If the secondaries were cleaned, operations to the
     * re-instated partition would encounter SecondaryIntegrityExceptions.
     *
     * The cleaner is also run on the target if a partition migration has
     * failed. This is to remove any secondary records that were generated on
     * the target during the failed migration. Secondary cleaning activity
     * resulting from a failed migration must be completed before the failed
     * migration target can re-start, otherwise the migration may encounter
     * SecondaryIntegrityExceptions when writing a record which already has
     * a (leftover) secondary record for it.  Once a SecondaryInfo cleaning
     * record is added to the SecondaryInfoMap no new migration target will
     * start (see busySecondaryCleaning()).
     *
     * Note that the check in busySecondaryCleaning() does not distinguish
     * between secondary cleaning on the target due to a failure or cleaning
     * on the source due to a success. Since it is unlikely for a migration
     * source to also be a target it's not a worth being able to tell the
     * difference. Also, busySecondaryCleaning() only needs to check if the
     * migration target is a restart (i.e. the partition is the same as a
     * failed one) but we do not keep this information.
     *
     * Partition migration target cannot run if there is ongoing or pending
     * secondary cleaning.
     */
    private SecondaryInfoMap cleanSecondary(SecondaryInfoMap infoMap,
                                            Database infoDb)
            throws InterruptedException {
        if (exitMaintenance()) {
            return infoMap;
        }

        logger.info("Running secondary cleaning");

        /*
         * If there is other pending work, just make one pass. Note that these
         * values can't change will in this routine, so it is OK to be static.
         */
        final boolean workPending = infoMap.secondaryNeedsPopulate() ||
                                    infoMap.tableNeedCleaning();

        /*
         * The working secondary. Working on one secondary at a time is
         * an optimization in space and time. It reduces the calls to
         * getNextSecondaryToClean() which iterates over the indexes, and
         * makes it so that only one SecondaryInfo is keeping track of
         * the cleaned partitions.
         */
        String currentSecondary = null;

        do {
            Transaction txn = null;
            try {
                /**
                 * Wait for migration source activity to stop in two steps.
                 * First wait without setting the busy maintenance flag. This
                 * will allow source activity to continue (which is likely
                 * if source threads > 1). This also means cleaning will be
                 * delayed until all migration is done, reducing the number
                 * of passes over the secondary databases. Note that we only
                 * wait on source idle, target synchronization is handled
                 * via SecondaryInfo data (see comment above).
                 *
                 * If there is other work to do, we set the waiter to null which
                 * will cause awaitSourceIdle() to return without waiting.
                 */
                if (!repNode.getMigrationManager().
                                     awaitSourceIdle(workPending ? null : this)) {
                    break;
                }

                /*
                 * Source activity is done, or at least paused. Set busy and
                 * check for source or target activity.
                 */
                tableManager.setBusyMaintenance();
                repNode.getMigrationManager().awaitIdle(this);
                if (exitMaintenance()) {
                    break;
                }

                /*
                 * Note that we do not call setBusyMaintenance() because because
                 * we are only concerned about migration target restart and
                 * that is handled via SecondaryInfo date (see comment above).
                 */
                txn = repEnv.beginTransaction(null, CLEANER_CONFIG);

                infoMap = SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                if (currentSecondary == null) {
                    currentSecondary = infoMap.getNextSecondaryToClean();

                    if (currentSecondary != null) {
                        logger.log(Level.FINE, "Cleaning {0}",
                                   currentSecondary);
                    }
                }

                /* If no more, we are finally done */
                if (currentSecondary == null) {
                    logger.info("Completed cleaning secondary database(s)");
                    break;
                }

                final SecondaryInfo info =
                    infoMap.getSecondaryInfo(currentSecondary);
                assert info != null;
                assert info.needsCleaning();

                final SecondaryDatabase db =
                    tableManager.getSecondaryDb(currentSecondary);
                /*
                 * We have seen missing DB in a stress test. By throwing
                 * DNFE the thread will retry after a delay. If the DB
                 * was not yet initialized the delay may allow the DB to
                 * be opened/created. If that is not the problem, the retry
                 * loop will eventually throw the DNFE out causing the RN to
                 * restart. That should fix things since the problem here
                 * is between the secondary info and the table metadata.
                 * On restart the secondary info will be updated from the
                 * metadata before the maintenance thread is started.
                 */
                if (db == null) {
                    throw new DatabaseNotFoundException(
                                    "Failed to clean " + currentSecondary +
                                    ", the secondary database was not" +
                                    " found");
                }

                assert TestHookExecute.doHookIfSet(tableManager.cleaningHook,
                                                   db);

                if (!db.deleteObsoletePrimaryKeys(info.getLastKey(),
                                                  info.getLastData(),
                                                  CLEAN_BATCH_SIZE)) {
                    logger.log(Level.FINE, "Completed cleaning {0}",
                               currentSecondary);
                    info.doneCleaning();
                    currentSecondary = null;
                }
                infoMap.persist(infoDb, txn);
                txn.commit();
                txn = null;

                /* Track this activity */
                repNode.getAggregateThroughputTracker().
                                      addWriteBytes(CLEAN_BATCH_SIZE * 1024, 0);
            } catch (SecondaryIntegrityException sie) {
                TxnUtil.abort(txn);
                txn = null;

                if (tableManager.resetSecondary(infoDb, sie,
                                                "secondary cleaning",
                                                repEnv)) {
                    requestUpdate(true);
                }
            } finally {
                tableManager.clearBusyMaintenance();
                TxnUtil.abort(txn);
            }
        } while (!exitMaintenance() && !workPending);

        return infoMap;
    }

    /**
     * Cleans primary records. This will remove primary records associated
     * with a table which has been marked for deletion.
     */
    private SecondaryInfoMap cleanPrimary(SecondaryInfoMap infoMap,
                                          Database infoDb) {

        if (exitMaintenance()) {
            return infoMap;
        }
        /*
         * There is a race condition which can lead to records from a dropped
         * table to get left in the store. What can happen is:
         *  1) a write operation checks whether the table exist by calling
         *     getTable() which returns true
         *  2) a metadata update comes in indicating that the table is dropped
         *  3) cleaning starts
         *  4) the operation completes
         *
         * If the record written by the operation is behind the cleaning
         * scan (lower key), it will not be cleaned and remain in the store
         * forever. The fix is to keep track of the number of active
         * operations per table metadata sequence #, and then wait for any
         * operation using an earlier metadata to complete.
         */
        assert lastSeqNum != 0;
        final int opsWaitedOn = repNode.awaitTableOps(lastSeqNum);
        logger.log(Level.INFO,
                   "Running primary cleaning, waited on {0} ops to complete",
                   opsWaitedOn);

        /*
         * Clean primary records by using record extinction. Record deletions
         * are not logged, so this operation can be performed in a short
         * transaction. JE asynchronously discards the extinct records.
         *
         * Note that this operation does not require interlocking with partition
         * migration.
         */
        assert repEnv.isRecordExtinctionAvailable();

        /* If there is other pending work, just make one pass */
        final boolean workPending = infoMap.secondaryNeedsPopulate()||
                                    infoMap.secondaryNeedsCleaning();
        do {
            Transaction txn = null;
            try {
                txn = repEnv.beginTransaction(null, CLEANER_CONFIG);

                infoMap = SecondaryInfoMap.fetch(infoDb, txn, LockMode.RMW);

                final DeletedTableInfo info = infoMap.getNextTableToClean();
                if (info == null) {
                    logger.info("Completed cleaning all tables");
                    break;
                }
                assert !info.isDone();

                final Set<String> dbNames = getPartitionDbNames(info);
                if (!dbNames.isEmpty()) {
                    final TableImpl table =
                        tableManager.getTableInternal(info.getTargetTableId());
                    assert table.isDeleting();

                    logger.log(Level.INFO,
                               "Discarding table record(s) for {0} {1}",
                               new Object[] {info.getTargetTableId(),
                                             table.getFullName()});

                    Scanner.discardTableRecords(repEnv, txn, dbNames, table);
                }
                info.setDone();
                infoMap.persist(infoDb, txn);
                txn.commit();
                txn = null;
            } catch (DatabaseNotFoundException dnfe) {
                TxnUtil.abort(txn);
                txn = null;

                /*
                 * This can happen due to async partition migration creating
                 * and removing target partition DBs. In this case we simply
                 * retry unless there is other work to do.
                 */
                if (workPending) {
                    break;
                }
                logger.log(Level.INFO,
                           "Partition database not found during " +
                           "cleaning, will retry in {0} ms: {1}",
                           new Object[] {LONG_RETRY_WAIT_MS,
                                         dnfe.getMessage()});
                waitInUpdate(LONG_RETRY_WAIT_MS);
            } finally {
                TxnUtil.abort(txn);
            }
        } while (!exitMaintenance() && !workPending);

        return infoMap;
    }

    /**
     * Returns the set of partition DB names that need to be cleaned.
     */
    private Set<String> getPartitionDbNames(DeletedTableInfo info) {
        final Set<String> names = new HashSet<>();
        /*
         * Start by getting the names of any partitions being migrated to this
         * node. We then get the partitions known to the RN. This order is
         * important to make sure we get all of the DBs. If the order was
         * reversed, a target migration could end after getting the RN's
         * names and before we query the migration targets and that DB would
         * be missed.
         * It is OK for a migration target to start after this point because
         * the records from any dropped table are being filtered out (see
         * MigrationTarget).
         */
        repNode.getMigrationManager().getTargetPartitionDbNames(names);

        for (PartitionId partition : repNode.getPartitions()) {
            if (info.isCompleted(partition)) {
                /* Should only happen if we mix approaches. */
                continue;
            }
            names.add(partition.getPartitionName());
        }
        return names;
    }
}
