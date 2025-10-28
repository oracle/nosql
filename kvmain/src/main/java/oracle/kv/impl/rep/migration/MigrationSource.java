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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static oracle.kv.impl.api.ops.InternalOperationHandler.getStorageSize;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.ops.ResourceTracker;
import oracle.kv.impl.rep.PartitionManager;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.migration.TransferProtocol.OP;
import oracle.kv.impl.rep.migration.generation.PartitionGeneration;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.WaitableCounter;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.net.DataChannel;

/**
 * Migration source thread. This thread will read records from the Db and
 * send them along to the target. It will exit when the reading of the Db is
 * complete or the migration is canceled.
 *
 * Partition DB Transfer (Partition scan)
 *
 * The initial data transfer takes place on the channel passed to the
 * constructor. The communication on this channel is one-way (source to target)
 * and consists of messages defined by the OP enum.
 *
 * {@literal
 *                               lastKey
 *               <-- "behind"       |       "ahead" -->
 *                                  V
 *   |-------------|*********************************|-------------|
 * lowest        first                             last          highest
 *  key         record   --- key-order scan -->   record          key
 * }
 *
 * In general, during the data transfer, lastKey is the current "position" of
 * the transfer. When a client operation comes it, its key is compared to
 * lastKey. If the key is less than lastKey, "behind" the transfer, the
 * operation is immediately sent to the target via the migration stream.
 * If the key is "ahead" of the transfer the operation is not immediately
 * sent since it will eventually be picked up by the scan. More details are
 * provided below.
 *
 * Transfer of Ownership (ToO)
 *
 * Once the transfer is complete, the source and target nodes participate in
 * the Transfer of Ownership (ToO) protocol to establish the partition at its
 * new location.
 *
 * {@literal
 * Transfer of Ownership (ToO)
 *
 * #     Source                            Target                     Admin
 * 0 Servicing client OPS COPY(K/V)    Processing OPS               Polling for
 *   Sending on-disk K/V  PUT(K/V)  -> from source         <- Done? completion.
 *   and client ops       DELETE(K)                    No ->
 *   Topology is at TA
 * 1 Read last K/V from
 *   on-disk P
 * 2 Persist TAx
 *   (partition DB closed)
 * 3 End Of Data                EOD ->
 * 4 Request handler            RMW ->
 *   forwards client ops
 * 5                                    Make partition durable
 * 6                                    Persist TAx
 * 7                                    Accept RMW
 * 8                                                        <- Done?
 * 9                                                   Yes ->
 * 10                                                                Update
 *                                                                   topology
 *                                                                   TA => TB
 * 11 Update to TB                      Update to TB
 * 12 Stop RMW
 * }
 *
 * In #0 a No response to Done? is PENDING or RUNNING
 * In #9 a Yes response to Done? is SUCCESS
 *
 * Forwarded client operations at #4 will fail until the target reaches #7.
 *
 * After #7 the migration can not be canceled because the target partition
 * may have been modified by forwarded client operations and therefore is the
 * only up-to-date copy of the data.
 *
 * To cancel a migration the admin can invoke cancelMigration(PartitionId) on
 * the target. The admin can consider the migration canceled Iff the return
 * value is PartitionMigrationState.ERROR. When the migration is canceled, the
 * admin must then invoke canceled(PartitionId, RepGroupId) on the source
 * repNode.
 *
 * After #2 the source will monitor the target by periodically invoking
 * getMigrationState(). If PartitionMigrationState.ERROR is returned the source
 * will remove the record for the migration, undoing step #2 and effectively
 * canceling the migration on the source side. (see TargetMonitorExecutor)
 */
public class MigrationSource implements Runnable {

    private final static int TXN_WAIT_POLL_PERIOD_MS = 50;

    private final static int TXN_WAIT_TIMEOUT_MS = 5000;

    private final static long WAIT_PENDING_OPS_TIMEOUT_MS = 10 * 60 * 1000;

    /**
     * Test hook that throws an IOException before sending the EOD,
     * setting the target migration state to ERROR.
     */
    public static TestHook<PartitionId> eodSendFailureHook = null;

    /**
     * Test hook to inhibit calling manager.monitorTarget() to cancel
     * initialising the TargetMonitorExecutor in persistTransferComplete.
     */
    public static TestHook<PartitionId> noMonitorTargetHook = null;

    /**
     * Test hook that can wait to simulate waiting for pending operations
     * with no-op stream migration handler to complete.
     */
    public static TestHook<PartitionId> pendingOperationHook = null;

    private final Logger logger;

    private final DataChannel channel;

    private final DataOutputStream stream;

    /* The partition this source is transferring */
    private final PartitionId partitionId;

    /* The target this partition will be transfered to */
    private final RepNodeId targetRNId;

    private final RepNode repNode;

    private final MigrationService service;

    /* The partition db */
    private final Database partitionDb;

    /*
     * This keeps track of the prepared txns, allowing the ToO protocol to
     * wait until all pending client operations have be resolved and sent
     * to the target. The counter is incremented on a successful prepare, and
     * decremented when a commit is sent. The EOD will be sent once the
     * counter is at zero.
     */
    private final WaitableCounter txnCount = new WaitableCounter();

    /*
     * True if this is a transfer only stream and the partition is not actually
     * migrated(e.g., in elastic search). In this mode there is no need persist
     * transfer complete. Also in this mode, the VLSN is sent. The mode is
     * enabled when the target is TransferProtocol.TRANSFER_ONLY_TARGET.
     */
    private final boolean transferOnly;

    /*
     * The thread executing the source runnable. Need to keep this because
     * we may have to wait for the thread to exit on shutdown.
     */
    private volatile Thread executingThread = null;

    /*
     * The last key sent. This is used to filter client operations. Client
     * operations less than or equal to the lastKey must be sent to the target.
     * All other ops can be ignored. If lastKey is null, all client operations
     * are sent. This happens when the partition scan is complete. Synchronize
     * when accessing this field.
     */
    private DatabaseEntry lastKey = new DatabaseEntry(new byte[0]);

    /*
     * The minimum key of client operations that were not transferred because
     * they were greater than lastKey, or null if no such operations.
     * Synchronize when accessing this field.
     */
    private DatabaseEntry minKey = null;

    /* True if the migration has been canceled */
    private volatile boolean canceled = false;

    /* True if EOD has been sent */
    private volatile boolean eod = false;

    /* statistics */
    private final long startTime;
    private long endTime = 0;
    private int operations = 0;
    private int filtered = 0;
    private int transactionConflicts = 0;
    private long recordsSent = 0;
    private long clientOpsSent = 0;
    private int cursorRewinds = 0;

    /**
     * a set-once variable that would be set to true when the migration
     * completes, false otherwise.
     * */
    private volatile boolean migrationDone = false;

    MigrationSource(DataChannel channel,
                    PartitionId partitionId,
                    RepNodeId targetRNId,
                    RepNode repNode,
                    MigrationService service,
                    Params params)
        throws IOException {
        this.channel = channel;
        this.stream = new DataOutputStream(Channels.newOutputStream(channel));
        this.partitionId = partitionId;
        this.targetRNId = targetRNId;
        this.transferOnly =
            targetRNId.equals(TransferProtocol.TRANSFER_ONLY_TARGET);
        this.repNode = repNode;
        this.service = service;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        partitionDb = repNode.getPartitionDB(partitionId);

        channel.configureBlocking(true);
        channel.socket().setSoTimeout(
                params.getRepNodeParams().getReadWriteTimeout());
        channel.socket().setTcpNoDelay(false);
        startTime = System.currentTimeMillis();
    }

    /**
     * Gets statistics on this migration source.
     *
     * @return a statistics object
     */
    PartitionMigrationStatus getStatus() {
        return new PartitionMigrationStatus(partitionId.getPartitionId(),
                                            targetRNId.getGroupId(),
                                            repNode.getRepNodeId().getGroupId(),
                                            operations,
                                            startTime,
                                            endTime,
                                            recordsSent,
                                            clientOpsSent);
    }

    int getTargetGroupId() {
        return targetRNId.getGroupId();
    }

    boolean isTransferOnly() {
        return transferOnly;
    }

    /**
     * Returns true if there is a thread associated with this source.
     * Note that this doesn't mean that the thread is running.
     *
     * @return true if there is a thread associated with this source
     */
    boolean isAlive() {
        return (executingThread != null);
    }

    /**
     * Cancels the migration. If wait is true it will wait for the source
     * thread to exit. Otherwise it returns immediately.
     *
     * @param wait if true will wait for thread exit
     */
    synchronized void cancel(boolean wait) {
        canceled = true;

        if (!wait) {
            return;
        }

        final Thread thread = executingThread;

        /* Wait if there is a thread AND is is running */
        if ((thread != null) && thread.isAlive()) {
            assert Thread.currentThread() != thread;

            try {
                logger.log(Level.FINE, "Waiting for {0} to exit", this);
                thread.join(5000);

                if (isAlive()) {
                    logger.log(Level.FINE, "Cancel of {0} timed out", this);
                }
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    /**
     * Logs the specified IO exception and cancels this source.
     *
     * @param ioe an IO exception
     */
    private void error(IOException ioe) {
        error("Unexpected exception, stopping " + this, ioe);
    }

    /**
     * Logs the specified exception with a message and cancels this source.
     *
     * @param message a message to log
     * @param ex an exception
     */
    private void error(String message, Exception ex) {
        logger.log(Level.INFO, message, ex);
        cancel(false);
    }

    /**
     * Scans the database and sends the entries while also handling concurrent
     * client operations.
     *
     * <h1>Synchronization</h1>
     * There are multiple entities that sends entries: here in the while loop
     * and the client operations. We must serialize the send events which is
     * achieved by synchronizing on this object.
     *
     * <h1>Coordination of Scan and Concurrent Client Operations.</h1>
     * The concurrent event of scan and client operations must also be
     * coordinated such that the order of the client operation is preserved.
     * That is, for the same key K,
     * (1) For two events (i) scan on K, corresponding to a send S1 and (ii) a
     * client operation on C, corresponding to a send S2. S1 must be ordered
     * before S2.
     * (2) For two client operations C1 and C2 on K, corresponding to S1 and
     * S2. If C1 commits to the database before C2, then S1 must be ordered
     * before S2.
     *
     * However, we do not want to put the scan (cursor) operation inside the
     * synchronization block for the send since that would greatly affect the
     * performance of the client operation.
     *
     * We achieve this coordination with lastKey and minKey. Psudo-code is
     * listed as follows:
     * <pre>
     * {@code
     *      while () {
     *          record = rewind ? cursor.get(SEARCH_GTE, lastKey) :
     *                            cursor.get(NEXT) :
     *          sync() {
     *              if (record.key >= minKey) {
     *                  lastKey = minKey
     *                  rewind = true
     *                  continue
     *              }
     *              send(record);
     *              lastKey = record.key
     *              rewind = false
     *          }
     *      }
     * }
     * {@code
     *      sync delete(op) {
     *      	if (op.key > lastKey) {
     *      		minKey = min(minKey, op.key)
     *      	}
     *      	sendDelete(op)
     *      }
     * }
     * {@code
     *      sync put(op) {
     *          if (op.key > lastKey) {
     *       		minKey = min(minKey, op.key)
     *       		return
     *          }
     *          sendPut(op)
     *     }
     * }
     * </pre>
     */
    @Override
    public void run() {
        executingThread = Thread.currentThread();

        final ResourceTracker tracker = repNode.getAggregateThroughputTracker();

        Cursor cursor = null;
        boolean rewind = false;

        try {
            /*
             * At and after this moment, all incoming write operation would
             * have a valid migration stream handler (non-noop). Wait for
             * pending ops with noop handler to zero therefore all the writes
             * will be visible to partition scanner. If times out, the
             * migration would fail and be retried later.
             */
            assert targetRNId.equals(new RepNodeId(0, 0)) /* unit test */ ||
                   service.getSource(partitionId) != null :
                "Migration source assert failure on pid=" + partitionId +
                ", targetRN=" + targetRNId;
            waitForPendingWrites();

            cursor = partitionDb.openCursor(null, CursorConfig.DEFAULT);
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry value = new DatabaseEntry();

            /* Migration must copy tombstones */
            final ReadOptions readOptions =
                         LockMode.DEFAULT.toReadOptions().clone().
                                             setExcludeTombstones(transferOnly);

            while (!canceled) {
                assert TestHookExecute.doHookIfSet(service.readHook, lastKey);
                assert cursor != null;

                try {
                    /*
                     * If rewind is true, key has been set to the lowest key
                     * above the previous lastKey that we need to rewind to.
                     * The key might have been deleted, so find that key or the
                     * next highest one.
                     */
                    final OperationResult result =
                                         cursor.get(key, value,
                                                    rewind ? Get.SEARCH_GTE :
                                                             Get.NEXT,
                                                    readOptions);
                    rewind = false;

                    if (result != null) {
                        /* Track the cost for load management */
                        tracker.addReadBytes(getStorageSize(cursor), false);

                        /*
                         * If the scanned record is for a dropped table, skip
                         * sending it.
                         */
                        if (MigrationManager.isForDroppedTable(repNode,
                                                               key.getData())) {
                            continue;
                        }

                        rewind = sendCopy(cursor, key, value, result);
                        continue;
                    }
                    /*
                     * Check if any client operations occurred ahead of
                     * lastKey. If so rewind to get them. If not, lastKey
                     * is set to null and transfer is done.
                     */
                    synchronized (this) {
                        if (minKey != null) {
                            /* Will set minKey to null */
                            setRewind(key);
                            rewind = true;
                            continue;
                        }

                        /*
                         * Done. Set lastKey to null so that all client
                         * operations will be forwarded to the target. Client
                         * operations are forwarded until the local topology
                         * is updated in transferComplete(). KVSTORE-1244
                         */
                        lastKey = null;
                        sendLastRecordMarker();
                    }

                    /*
                     * Must close cursor here because transfer complete
                     * will cause the underlying DB to be closed.
                     */
                    cursor.close();
                    cursor = null;

                    /* ToO #1 - Finished reading on-disk records */
                    transferComplete();
                    return;
                } catch (LockConflictException lce) {
                    if (cursor == null) {
                        return;
                    }

                    /* retry */
                    transactionConflicts++;
                }
            }
        } catch (DatabaseException de) {
            logger.log(Level.INFO,
                     this + " encountered database exception reading partition",
                     de);
        } catch (Exception ex) {
            logger.log(Level.INFO,
                       this + " encountered unexpected exception", ex);
        } finally {
            logger.log(Level.INFO, "{0} exiting", this);
            closeChannel();
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException ex) {
                    logger.log(Level.WARNING,
                               "Exception closing partition migration cursor",
                               ex);
                }
            }
            migrationDone = true;
            executingThread = null;
        }
    }

    private void sendLastRecordMarker() {
        try {
            writeOp(OP.LAST_RECORD_MARKER);
            logger.log(Level.INFO,
                       "Sent last record marker for {0}", partitionId);
            stream.flush();
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    /**
     * Closes the channel, logging any resulting exceptions.
     */
    private void closeChannel() {
        try {
            channel.close();
        } catch (IOException ioe) {
            logger.log(Level.WARNING,
                       "Exception closing partition migration channel", ioe);
        }
    }

    /**
     * Returns true if the migration is complete, false otherwise
     */
    boolean isMigrationDone() {
        return migrationDone;
    }

    /**
     * Waits for the pending no-op stream migration handler writes counter to
     * count down to zero so that we can be sure all pending writes are either
     * visible to partition scanner or aborted.
     * @throws TimeoutException if times out in waiting
     */
    private void waitForPendingWrites() throws TimeoutException {
        if (pendingOperationHook != null) {
            /* run test hook in unit test only */
            assert TestHookExecute.doHookIfSet(pendingOperationHook,
                                               partitionId);
            return;
        }

        //TODO: the waiting here implies an underlying assumption that
        // migration is considered to have lower priority than regular
        // write operations in request handler, and therefore we make the
        // migration wait for the pending writes to complete. This assumption
        // is subject to further discussion, and it might change in future.

        /* normal case */
        final MigrationManager mm = repNode.getMigrationManager();
        final WaitableCounter wc = mm.getNoopHandlerWrites(partitionId);
        final int pending = (wc == null) ? 0 : wc.get();
        if (wc == null || pending == 0) {
            logger.info("No pending writes for partition=" + partitionId);
            return;
        }

        logger.info("Start waiting for #pending writes=" + pending +
                    ", partition=" + partitionId);
        final long start = System.currentTimeMillis();
        if (wc.awaitZero(TXN_WAIT_POLL_PERIOD_MS,
                         WAIT_PENDING_OPS_TIMEOUT_MS)) {
            logger.info("Completed all #pending=" + pending +
                        " writes with no-op stream handler" +
                        ", partition=" + partitionId +
                        ", elapsed timeMs=" +
                        (System.currentTimeMillis() - start) +
                        ", timeoutMs=" + WAIT_PENDING_OPS_TIMEOUT_MS);
            return;
        }

        throw new TimeoutException("Timeout or interrupted in waiting" +
                                   ", #pending=" + pending +
                                   ", #remaining=" + wc.get() +
                                   ", timeoutMs=" +
                                   WAIT_PENDING_OPS_TIMEOUT_MS +
                                   ", pid=" + partitionId);
    }

    /**
     * Signals the transfer of the on-disk records to the target is complete.
     */
    private void transferComplete() {
        logger.log(Level.INFO, "{0} completed transfer", this);
        endTime = System.currentTimeMillis();

        /*
         * ToO #2 - Persist the fact that the transfer to the target is
         * complete. This will update the local topology which will close
         * the partition DB and will redirect all ops to the new rep group
         * on the target.
         *
         * When the transfer completed update propagates to the replicas they
         * will start to forward their client ops to the target - ToO #4
         *
         * If this is in transfer only mode, no need to persist anything.
         */
        if (!transferOnly && !persistTransferComplete()) {
            cancel(false);
            return;
        }

        /*
         * Check, and briefly wait if needed, for resolutions to be sent
         * for any prepared txns. Since persistTransferComplete() will stop
         * new client operations from starting, the count should reach 0.
         */
        if (!txnCount.awaitZero(TXN_WAIT_POLL_PERIOD_MS, TXN_WAIT_TIMEOUT_MS)) {
            logger.log(Level.INFO, "Waiting to resolve prepared txns for {0} " +
                                   "timed-out, current count: {1}",
                       new Object[]{partitionId, txnCount.get()});
        }

        /*
         * ToO #3 - Write the End of Data marker onto the migrations stream.
         * This will set the eod flag causing any in-progress client write ops
         * to fail. Once the local topology is updated the client ops will be
         * redirected.
         */

        /* read the last closed generation and send EOD */
        long endVLSNSeq = 0;
        int genNum = 0;
        if (!transferOnly) {
            final PartitionManager pm = repNode.getPartitionManager();
            final PartitionGenerationTable pgt = pm.getPartGenTable();
            final PartitionGeneration pg = pgt.getLastGen(partitionId);
            endVLSNSeq = pgt.getLastVLSN();
            genNum = pg.getGenNum().getNumber();
        }

        /* run test hook in unit test only */
        assert TestHookExecute.doHookIfSet(eodSendFailureHook, partitionId);

        sendEOD(endVLSNSeq, genNum);

        if (eod /* eod successfully sent */ &&
            logger.isLoggable(Level.INFO)) {
            final long seconds = (endTime - startTime) / 1000;
            final long opsPerSec =
                (seconds == 0) ? operations : operations / seconds;
            logger.log(Level.INFO,
                       "Sent EOD for {0}, {1} total operations, {2} " +
                       "filtered, {3} txn conflicts, " +
                       "{4} rewinds, {5} ops/second, " +
                       "endVLSN={6}, generation#={7}",
                       new Object[]{partitionId, operations,
                           filtered, transactionConflicts,
                           cursorRewinds, opsPerSec, endVLSNSeq, genNum});
        }

        /*
         * At this point, there is a small chance that the migration would
         * be canceled and retried. For example, if the target crashes
         * before receives the EOD from source, the whole migration may have
         * to retry. Therefore, we should not close the partition generation
         * here, instead, the generation should be closed when the partition db
         * is dropped after the global topology is updated.
         */
    }

    /* -- Partition transfer -- */

    /**
     * Sends a partition record. Returns true, and skips sending the record, if
     * the transfer needs to be rewound. If true is returned, key is set to the
     * rewind target.
     */
    private synchronized boolean sendCopy(Cursor cursor,
                                          final DatabaseEntry key,
                                          final DatabaseEntry value,
                                          OperationResult result) {

        /*
         * If an operation occurred at or behind the get (minKey <= key) we
         * must rewind the cursor so that the op is transfered (or in the case
         * of delete, the send is dropped).
         */
        if (minKey != null && partitionDb.compareKeys(minKey, key) <= 0) {
            /* Will set minKey to null */
            setRewind(key);
            return true;
        }
        minKey = null;

        if (transferOnly) {
            assert !result.isTombstone();
            sendCopy(key, value,
                    getVLSNFromCursor(cursor, false),
                    0L /*creationTime*/,
                    0L /*modificationTime*/,
                    result.getExpirationTime(),
                    false /*isTombstone*/);
        } else {
            sendCopy(key, value,
                     0L /*vlsn*/,
                     result.getCreationTime(),
                     result.getModificationTime(),
                     result.getExpirationTime(),
                     result.isTombstone());
        }
        assert lastKey != null;
        lastKey.setData(key.getData());
        return false;
    }

    private void sendCopy(DatabaseEntry key,
                          DatabaseEntry value,
                          long vlsn,
                          long creationTime,
                          long modificationTime,
                          long expirationTime,
                          boolean isTombstone) {
        assert Thread.holdsLock(this);
        try {
            writeOp(OP.COPY);
            writeDbEntry(key);
            writeDbEntry(value);
            writeCreationTime(creationTime);
            writeModificationTime(modificationTime);
            writeExpirationTime(expirationTime);
            writeTombstoneFlag(isTombstone);
            writeVLSN(vlsn);
            recordsSent++;
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    /**
     * Sets key to the rewind target (minKey). Also sets lastKey to
     * minKey.
     * <p>
     * There is progress concern regarding rewind. If the application
     * repeatedly writes the same key during partition migration that causes,
     * a rewind the migration may never finish. This is highly unlikely with
     * the key factor being that the application should not be able to
     * generate such a high throughput load competing the direct scan of the
     * partition database.
     */
    private void setRewind(final DatabaseEntry key) {
        assert Thread.holdsLock(this);
        /*
         * The cursor will use key as input to the search. Set it, and
         * lastKey to the minKey.
         */
        key.setData(minKey.getData());
        lastKey.setData(minKey.getData());
        minKey = null;
        cursorRewinds++;
    }

    /* -- Clien operations -- */

     /**
     * Sends a client put operation. The operation is not sent if the put
     * key is greater than lastKey.
     *
     * The following case needs special consideration. If a put comes in
     * for a key greater than lastKey and less than or equal to the get()
     * we must rewind the cursor in order to pickup the client put operation.
     * This is accomplished by rewinding the cursor back to the  put key
     * (minKey is set to the put key in filterOp()).
     *
     *     lastKey          cursor.get()
     *        |                 |
     *        v                 v
     *     -----------------------------
     *                 ^
     *                 |
     *                put
     * <p>
     * In this case minKey is set to the put key.
     */
    synchronized boolean sendPut(long txnId,
                                 DatabaseEntry key,
                                 DatabaseEntry value,
                                 long vlsn,
                                 long creationTime,
                                 long modificationTime,
                                 long expirationTime,
                                 boolean isTombstone) {
        if (canceled) {
            return false;
        }
        if (filterOp(key)) {
            filtered++;
            return false;
        }

        try {
            writeOp(OP.PUT, txnId);
            writeDbEntry(key);
            writeDbEntry(value);
            writeCreationTime(creationTime);
            writeModificationTime(modificationTime);
            writeExpirationTime(expirationTime);
            writeTombstoneFlag(isTombstone);
            writeVLSN(vlsn);
            clientOpsSent++;
            return true;
        } catch (IOException ioe) {
            error(ioe);
        }
        return false;
    }

    /**
     * Sends a client delete operation.
     *
     * As with put, if a delete comes in for a key greater than lastKey and
     * less than or equal to the get() we must rewind the cursor. This is
     * accomplished by rewinding the cursor back to the delete key (minKey
     * is set to the delete key in filterOp()).
     *
     *     lastKey          cursor.get()
     *        |                 |
     *        v                 v
     *     -----------------------------
     *                          ^
     *                          |
     *                       delete
     *
     *  In this case the minKey is set to the delete key.
     */
    synchronized boolean sendDelete(long txnId, DatabaseEntry key,
                                    Cursor cursor) {
        if (canceled) {
            return false;
        }
        /*
         * Deletes are always sent because the vlsn is needed for the transfer
         * only case. The vlsn is not available during rewind, as the record
         * is no longer present. The call to filterOp() will set minKey if
         * needed.
         */
        filterOp(key);

        /* get vlsn from cursor if transfer only mode */
        final long vlsn;
        if (cursor == null || !transferOnly) {
            vlsn = 0L;
        } else {
            vlsn = getVLSNFromCursor(cursor, true);
        }

        try {
            writeOp(OP.DELETE, txnId);
            writeDbEntry(key);
            writeVLSN(vlsn);
            clientOpsSent++;
            return true;
        } catch (IOException ioe) {
            error(ioe);
        }
        return false;
    }

    /**
     * Returns true if the specified key can be filtered from the migration
     * stream.
     */
    private boolean filterOp(DatabaseEntry key) {
        assert Thread.holdsLock(this);

        /*
         * If the partition transfer has eneded (lastKey == null) then we
         * must seend all ops.
         */
        if (lastKey == null) {
            return false;
        }

        /*
         * compareKeys() can throw an ISE if the database has been closed (due
         * to the migration completing). In this case the ISE will be caught in
         * RequestHandlerImpl.executeInternal() and the client operation will
         * be forwarded
         */

        /*
         * If the op is ahead of the transfer (key > lastKey) then we can
         * skip sending it.
         */
        if (partitionDb.compareKeys(key, lastKey) > 0) {

            /*
             * We record the minimum key that was skipped in case the
             * transfer just happens to scan past that point.
             */
            if (minKey == null ||
                partitionDb.compareKeys(key, minKey) < 0) {
                minKey = new DatabaseEntry(key.getData());
            }
            return true;
        }
        return false;
    }

    synchronized void sendPrepare(long txnId) {
        if (canceled) {
            return;
        }
        try {
            writeOp(OP.PREPARE, txnId);
            txnCount.incrementAndGet();
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    synchronized void sendResolution(long txnId,
                                     boolean commit,
                                     boolean prepared) {
        if (canceled) {
            return;
        }

        /*
         * If EOD has been sent, we cannot send the resolution, however there
         * is one case where it is OK to ignore this situation.
         */
        if (eod) {

            /*
             * If the operation was prepared, the target will fail and the
             * migration aborted. If not prepared, then the client operation
             * failed with an exception and the txn would have been aborted.
             * In this situation the target will toss the operation when
             * EOD is received. Therefore we can safely ignore this case.
             */
            if (prepared || commit) {
                logger.info("Unable to send resolution for prepared txn, " +
                            "past EOD, stopping");
                cancel(false);
            } else {
                logger.fine("Unable to send ABORT for unresolved txn " +
                            "(past EOD), ignoring");
            }
            return;
        }

        try {
            writeOp(commit ? OP.COMMIT : OP.ABORT, txnId);

            /* If the op was prepared the txnCount was incremented */
            if (prepared) {
                txnCount.decrementAndGet();
            }
        } catch (IllegalStateException ise) {

            /*
             * This should not happen since we have already checked for EOD
             */
            error("Unexpected exception attempting to send resolution, " +
                  "stopping " + this, ise);
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    private synchronized void sendEOD(long endVLSNSeq, int genNum) {
        if (canceled) {
            return;
        }
        try {
            writeOp(OP.EOD);
            if (!transferOnly) {
                stream.writeLong(endVLSNSeq);
                stream.writeInt(genNum);
            }
            /*
             * Make sure that the EOD is delivered right away to avoid a
             * timeout on the target. Once the EOD is received, the target will
             * disable the stream timeout, but it expects timely data until
             * then.
             */
            stream.flush();

            eod = true;
        } catch (IOException ioe) {
            error(ioe);
        }
    }

    private void writeOp(OP op) throws IOException {
        assert Thread.holdsLock(this);

        if (eod) {
            /* If transfer mode, just ignore this situation. */
            if (transferOnly) {
                return;
            }
            /*
             * Attempt to write an op after EOD has been sent. By throwing
             * an IllegalStateException RequestHandlerImpl.executeInternal()
             * will forward the client request to the new node.
             */
            throw new IllegalStateException(
                String.format(
                    "Migration of %s is completed with %s: "
                    + "startTime=%s, endTime=%s",
                    partitionId, this,
                    FormatUtils.formatTimeMillis(startTime),
                    FormatUtils.formatTimeMillis(endTime)));
        }
        try {
            stream.write(op.ordinal());
        } catch (IOException ioe) {
            /* If transfer mode, just ignore this situation. */
            if (transferOnly) {
                return;
            }
            throw ioe;
        }
        operations++;
    }

    private void writeOp(OP op, long txnId) throws IOException {
        writeOp(op);
        stream.writeLong(txnId);
    }

    private void writeDbEntry(DatabaseEntry entry) throws IOException {
        assert Thread.holdsLock(this);

        stream.writeInt(entry.getSize());
        stream.write(entry.getData());
    }

    private void writeVLSN(long vlsn) throws IOException {
        assert Thread.holdsLock(this);
        /* The VLSN is only written in transfer only mode */
        if (transferOnly) {
            stream.writeLong(vlsn);
        }
    }

    private void writeExpirationTime(long expTime) throws IOException {
        assert Thread.holdsLock(this);
        stream.writeLong(expTime);
    }

    private void writeCreationTime(long creationTime) throws IOException {
        assert Thread.holdsLock(this);
        /* creation time only needed for migration */
        if (!transferOnly) {
            stream.writeLong(creationTime);
        }
    }

    private void writeModificationTime(long modificationTime) throws IOException {
        assert Thread.holdsLock(this);
        /* modification time only need for migration */
        if (!transferOnly) {
            stream.writeLong(modificationTime);
        }
    }

    private void writeTombstoneFlag(boolean isTombstone) throws IOException {
        assert Thread.holdsLock(this);
        /* Only write the tombstone flag during migration */
        if (!transferOnly) {
            stream.writeBoolean(isTombstone);
        }
    }

    private long getVLSNFromCursor(Cursor cursor, boolean fetchLN) {
        if (cursor == null) {
            return NULL_VLSN;
        }
        return DbInternal.getCursorImpl(cursor)
                         .getCurrentVersion(fetchLN)
                         .getVLSN();
    }

    private boolean persistTransferComplete() {
        logger.log(Level.FINE,
                   "Persist transfer complete for {0}", partitionId);

        final RepGroupId sourceRGId =
                        new RepGroupId(repNode.getRepNodeId().getGroupId());
        final PartitionMigrationStatus status = getStatus();
        final TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setConsistencyPolicy(
                                 NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        txnConfig.setDurability(
               new Durability(Durability.SyncPolicy.SYNC,
                              Durability.SyncPolicy.SYNC,
                              Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

        final MigrationManager manager = service.manager;

        final Boolean success =
            manager.tryDBOperation(db -> {

                Transaction txn = null;
                try {
                    txn = db.getEnvironment().beginTransaction(null, txnConfig);

                    final PartitionMigrations pm =
                                      PartitionMigrations.fetchWrite(db, txn);

                    pm.add(pm.newSource(status, partitionId, sourceRGId,
                                        targetRNId));
                    pm.persist(db, txn, true);
                    txn.commit();
                    txn = null;
                    return true;
                } finally {
                    TxnUtil.abort(txn);
                }
            }, true);

        if ((success == null) || !success) {
            return false;
        }

        /*
         * The local topology must be updated to reflect the new location
         * of the partition before the source thread exits so that client
         * operations no longer access the local partition DB.
         */
        manager.criticalUpdate();

        /* run test hook in unit test only */
        assert TestHookExecute.doHookIfSet(noMonitorTargetHook, partitionId);
        if (noMonitorTargetHook != null) {
            return true;
        }

        /*
         * Now that the record has been persisted and the local topology
         * updated, monitor the target for failure so that this may be undone.
         */
        manager.monitorTarget();
        return true;
    }

    @Override
    public String toString() {
        return String.format(
            "MigrationSource(%s)[%s, %s, %s]",
            startTime, partitionId, targetRNId, eod);
    }
}
