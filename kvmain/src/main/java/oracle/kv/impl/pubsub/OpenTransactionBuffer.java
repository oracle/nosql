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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableVersionException;
import oracle.kv.impl.rep.migration.generation.PartitionGeneration;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamOperation.DeleteEvent;
import oracle.kv.pubsub.StreamOperation.PutEvent;
import oracle.kv.pubsub.StreamOperation.SequenceId;
import oracle.kv.pubsub.SubscribedTableVersionException;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.table.Table;
import oracle.kv.txn.TransactionIdImpl;

import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.util.PackedInteger;

/**
 * Object maintaining a list of open transactions received from source. When a
 * transaction message is received, the whole txn in openTxnBuffer will be
 * processed as a whole by client defined callback.
 * <p>
 * For each key transferred from the Partition Migration Service, openTxnBuffer
 * would treat each key as a single txn. The callback is fired for each txn.
 * The openTxnBuffer remembers VLSN of the last txn processed.
 */
public class OpenTransactionBuffer {

    /** test hook to throw exception */
    public static volatile ExceptionTestHook<DataEntry, Exception>
        expHook = null;

    /** test hook of min vlsn in queue */
    static volatile TestHook<AtomicLong> testHookMinVLSNInQueue = null;
    /** test hook of min vlsn in buffer */
    static volatile TestHook<AtomicLong> testHookMinVLSNInOtb = null;

    /** vlsn if the min vlsn is not available */
    static final long MAX_VLSN = Long.MAX_VALUE;
    /* timeout to poll input queue */
    private static final int INPUT_QUEUE_POLL_TIMEOUT_MS = 1000;

    /* private logger */
    private final Logger logger;
    /* parent consumer */
    private final ReplicationStreamConsumer parent;
    /* open txn buffer indexed by transaction ID */
    private final Map<Long, List<DataEntry>> openTxnBuffer;
    /* FIFO queue from which to dequeue messages from replication stream */
    private final BlockingQueue<? extends DataEntry> inputQueue;
    /* FIFO queue to which to enqueue messages for client */
    private final PublishingUnit.BoundedOutputQueue outputQueue;
    /* replication group this worker belongs to */
    private final RepGroupId repGroupId;

    /* true if user subscribes all tables */
    private final boolean streamAllTables;
    /*
     * Map of cached subscribed tables.
     *
     * The map is indexed by root table id string. Each value is a
     * synchronized map of child tables under the root table id, indexed by its
     * own table id string. If streaming is limited to a set of tables, then
     * the list of tables for the root table id string contains all
     * subscribed tables with that root table as their parent, possibly
     * including the root table. If streaming all tables, then the
     * list contains just the root table.
     *
     * If user specifies subscribed tables, it will be populated with cached
     * copy of subscribed tables when OTB is constructed, and any cached
     * table will be refreshed only when table version exception for that table
     * is raised during streaming. During refresh, if OTB detects that a
     * subscribed table is dropped at kvstore, copy of the dropped table will
     * be removed from cache. Since then OTB will no longer stream any ops
     * from the dropped table. Note even the user re-create a new table with the
     * same name later, it is not considered as the same subscribed table
     * that has been dropped, thus it wont be streamed.
     *
     * If user does not specify any subscribed table, OTB considers that user
     * would like to subscribe all tables at kvstore, both existing at the
     * time of subscription, and newly created tables during subscription.
     * OTB will not cache any table when OTB is constructed, instead, it will
     * populate the cache during streaming. If table version mismatch is
     * found, OTB will refresh cached table. During refresh if the table is
     * found to have been dropped, OTB will remove it from cache, and will no
     * longer stream any ops from the dropped tables. Note in this case, if
     * user re-create a new tale with the same table, the newly table will be
     * streamed since the user subscribes all the tables.
     *
     */
    private final ConcurrentMap<String, Map<String, TableImpl>> cachedTables;

    /*
     * Map of table id and table name of dropped table, if a table is dropped
     * and detected during refresh, OTB will not stream any ops from that table.
     *
     * Note that it is only queried when streamAllTables is true, as a way of
     * distinguishing a dropped table missing from cachedTables, from a brand
     * new table that is never cached before.
     *
     * This set could grow arbitrarily large over time as additional tables are
     * dropped. This is the cost we pay to support subscribe all tables,
     * until we are able to include DDL in the replication stream when we are
     * able to tell exactly when the table has been dropped.
     *
     */

     /*
      * TODO: an LRU cache with bounded size to clear out old entries, with
      * idea that entries for older dropped tables will eventually no longer
      * appear in the stream.
      */
    private final Set<String> droppedTables;

    /* statistics */

    /** VLSN of last txn committed */
    private volatile long lastCommitVLSN;
    /** VLSN of last txn aborted */
    private volatile long lastAbortVLSN;
    /* max # of open txns in buffer since creation */
    private final AtomicLong maxOpenTxn;

    /*
     * Below are incremental stats and record the change between updates.
     * Parent RSC will update these stats to PU periodically where they are
     * aggregated and added up to previous value.
     */

    /* # of committed ops */
    private final AtomicLong numCommitOps;
    /* # of aborted ops */
    private final AtomicLong numAbortOps;
    /* # of committed txns */
    private final AtomicLong numCommitTxn;
    /* # of aborted txns */
    private final AtomicLong numAbortTxn;

    /* worker thread for the buffer */
    private volatile TxnWorkerThread workerThread;

    /* processor to handle partition generation marker */
    private final PartitionGenMarkerProcessor partGenMarkProcessor;

    /* deserializer to create stream operations from JE data entries */
    private final StreamOperation.Deserializer deserializer;
    /** true if include abort transactions, false otherwise */
    private final boolean includeAbortTxn;

    OpenTransactionBuffer(ReplicationStreamConsumer parent,
                          RepGroupId repGroupId,
                          BlockingQueue<? extends DataEntry> inputQueue,
                          PublishingUnit.BoundedOutputQueue outputQueue,
                          Collection<TableImpl> tables,
                          StreamOperation.Deserializer deserializer,
                          Logger logger) {

        this.parent = parent;
        this.repGroupId = repGroupId;
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.logger = logger;
        this.deserializer = (deserializer == null) ?
            new DefaultDeserializer() : deserializer;

        /* make a map of local copy of subscribed tables for quick lookup */
        cachedTables = new ConcurrentHashMap<>();
        if (tables == null) {
            /*
             * All tables subscribed.  We will add root tables to the table
             * cache as we encounter their root table IDs.
             */
            streamAllTables = true;
            final String trace = "will stream all tables";
            logger.fine(() -> lm(trace));
        } else {
            streamAllTables = false;
            /* build map of subscribed tables */
            final StringBuilder trace =
                new StringBuilder("will stream tables: ");
            for (TableImpl t : tables) {
                final String rootTableId = getTableId(t.getTopLevelTable());
                addCachedTable(rootTableId, t);
                trace.append(t.getFullNamespaceName()).append("(table id: ")
                     .append(t.getIdString()).append(", ")
                     .append("root table id: ").append(rootTableId).append(")")
                     .append(", ");
            }
            logger.fine(() -> lm(trace.toString()));
        }
        droppedTables = new HashSet<>();

        openTxnBuffer = new ConcurrentHashMap<>();

        lastCommitVLSN = NULL_VLSN;
        lastAbortVLSN = NULL_VLSN;

        numAbortOps = new AtomicLong(0);
        numCommitOps = new AtomicLong(0);
        numCommitTxn = new AtomicLong(0);
        numAbortTxn = new AtomicLong(0);
        maxOpenTxn = new AtomicLong(0);
        partGenMarkProcessor = new PartitionGenMarkerProcessor(this, logger);

        /* to be initialized in startWorker() */
        workerThread = null;

        final PublishingUnit pu = parent.getPu();
        if (pu == null) {
            /* some unit test only */
            includeAbortTxn = false;
        } else {
            /* regular stream */
            includeAbortTxn = pu.includeAbortTransaction();
        }
    }

    public RepGroupId getRepGroupId() {
        return repGroupId;
    }

    /**
     * Unit test only
     */
    public boolean isWaitingForCkpt () {
        return partGenMarkProcessor.isWaitingForCkpt();
    }

    /**
     * Shut down otb completely
     */
    void close() {

        shutdownWorker();
        workerThread = null;

        clearTxnBuffer();
    }

    /**
     * Starts the worker thread
     */
    void startWorker() {
        /*
         * the worker thread is created and started at the start stream, and
         * re-retry in error
         */
        workerThread = new TxnWorkerThread(parent.getConsumerId());
        workerThread.start();
        logger.info(lm("Txn worker thread started"));
    }

    /**
     * Gets the VLSN of last committed transaction
     *
     * @return last commit VLSN
     */
    long getLastCommitVLSN() {
        return lastCommitVLSN;
    }

    /**
     * Gets VLSN of last aborted transaction
     *
     * @return last aborted VLSN
     */
    long getLastAbortVLSN() {
        return lastAbortVLSN;
    }

    long getOpenTxns() {
        return openTxnBuffer.size();
    }

    long getMaxOpenTxns(){
        return maxOpenTxn.get();
    }

    /* Update and refresh incremental stats */

    long getCommitTxns() {
        return numCommitTxn.getAndSet(0);
    }

    long getAbortTxns() {
        return numAbortTxn.getAndSet(0);
    }

    long getCommitOps() {
        return numCommitOps.getAndSet(0);
    }

    long getAbortOps() {
        return numAbortOps.getAndSet(0);
    }

    ReplicationStreamConsumer getParentRSC() {
        return parent;
    }

    /**
     * unit test only
     */
    PartitionGenMarkerProcessor getPartGenMarkProcessor() {
        return partGenMarkProcessor;
    }

    void addCachedTable(String rootTableId, TableImpl table) {
        cachedTables.computeIfAbsent(rootTableId,
                                     k -> Collections.synchronizedMap(
                                         new HashMap<>()))
                    .put(table.getIdString(), table);
    }

    void removeCachedTable(String rootTableId, String tableId) {
        cachedTables.computeIfPresent(rootTableId,
                                      (k, v) -> {
                                          v.remove(tableId);
                                          if (v.isEmpty()) {
                                              return null;
                                          }
                                          return v;
                                      });
    }

    /**
     * Returns vlsn below the minimal vlsn of entry in all transactions in
     * the transaction buffer and the output queue. If the buffer and the
     * queue are both empty, returns {@link #MAX_VLSN}.
     */
    long getMinVLSN() {
        /* compute the min vlsn in queue, null vlsn if queue is empty */
        final long queueMinVLSN = getMinVLSNInQueue();
        final long otbMinVLSN = getMinVLSNInOTB();
        final long vlsn = Math.min(otbMinVLSN, queueMinVLSN);
        /* if not max vlsn, return the previous vlsn of the minimal vlsn */
        if (vlsn == MAX_VLSN) {
            return MAX_VLSN;
        }
        return Math.max(VLSN.FIRST_VLSN, vlsn - 1);
    }

    /**
     * Enqueues a close partition generation
     * @param gen close partition generation
     * @param vlsn  vlsn of the entry
     * @throws InterruptedException if interrupted
     */
    void enqueueClosedGen(PartitionGeneration gen, long vlsn)
        throws InterruptedException {
        final int shardId = repGroupId.getGroupId();
        if (gen.isOpen()) {
            throw new IllegalArgumentException(
                "Generation to enqueue is open, shard=" + shardId +
                ", gen=" + gen);
        }

        final StreamOperation dummyOp =
            new ClosedPartGenStreamOp(shardId, vlsn, gen);
        enqueueMsg(dummyOp);
    }
    /**
     * Returns a vlsn that is smaller or equal to the minimal vlsn of all
     * operations in the output queue. They are committed txn and not yet
     * delivered. If the queue is empty, returns {@link #MAX_VLSN}.
     */
    private long getMinVLSNInQueue() {
        if (testHookMinVLSNInQueue != null) {
            /* unit test */
            return getValFromHook(testHookMinVLSNInQueue);
        }

        /* normal case */
        return outputQueue.getMinVLSNInQueue();
    }

    /**
     * Unit test only
     * <p>
     * Gets the value from test hook set by unit test
     * @param hook test hook
     * @return value from test hook
     */
    private long getValFromHook(TestHook<AtomicLong> hook) {
        final AtomicLong val = new AtomicLong();
        assert TestHookExecute.doHookIfSet(hook, val);
        return val.get();
    }

    /**
     * Returns minimal vlsn of all data entries in the open transaction
     * buffer, or {@link #MAX_VLSN} if the buffer is empty.
     */
    private long getMinVLSNInOTB() {
        if (testHookMinVLSNInOtb != null) {
            /* unit test */
            return getValFromHook(testHookMinVLSNInOtb);
        }

        /* normal case */
        long minVLSN = MAX_VLSN;
        for (List<DataEntry> txn : openTxnBuffer.values()) {
            if (txn.isEmpty()) {
                continue;
            }
            /* within txn, entries are ordered by vlsn */
            final DataEntry first = txn.get(0);
            final long vlsn = first.getVLSN();
            minVLSN = Math.min(minVLSN, vlsn);
        }
        return minVLSN;
    }

    private void replaceCachedTable(String rootTableId, TableImpl table) {
        cachedTables.computeIfPresent(rootTableId,
                                      (k, v) -> {
                                          v.put(table.getIdString(), table);
                                          return v;
                                      });
    }

    /**
     * Shutdown the worker thread
     */
    private void shutdownWorker() {
        shutDownWorkerThread(workerThread);
    }

    private void shutDownWorkerThread(StoppableThread worker) {
        if (worker == null) {
            logger.fine(() -> lm("Worker thread does not exist"));
            return;
        }
        if (!worker.isShutdown()) {
            worker.shutdownThread(logger);
        }
        logger.info(lm("Worker thread=" + worker.getName() + " shut down"));
    }

    /**
     * Refresh the buffer while keep the statistics
     * <p>
     * This is called when a RSC is trying to reconnect to the feeder due to
     * errors, e.g. master transfer. Parent RSC will guarantee that the new
     * connection will stream from the last committed VLSN to subscriber. All
     * unprocessed ops in buffer will be discarded and re-streamed from feeder.
     */
    private void clearTxnBuffer() {

        /* clear input queue */
        inputQueue.clear();

        /* clear all open txns in buffer */
        openTxnBuffer.clear();

        logger.fine(() -> lm("Txn buffer refreshed"));
    }

    /*
     * Adds an operation to openTxnBuffer. Create an open txn if it is the first
     * operation in this txn.
     *
     * @param entry   a data entry
     */
    private synchronized void addEntry(DataEntry entry) {
        final long txnid = entry.getTxnID();
        final List<DataEntry> txn =
            openTxnBuffer.computeIfAbsent(txnid, k -> new ArrayList<>());
        txn.add(entry);

        if (maxOpenTxn.get() < openTxnBuffer.size()) {
            maxOpenTxn.set(openTxnBuffer.size());
        }

        parent.getRSCStat()
              .setLastMsgTimeMs(NoSQLSubscriptionImpl.getCurrTimeMs());
    }

    /* Aborts a txn from openTxnBuffer */
    private synchronized void abort(final DataEntry entry)
        throws InterruptedException {

        assert (DataEntry.Type.TXN_ABORT.equals(entry.getType()));

        final long txnid = entry.getTxnID();
        final List<DataEntry> txn = openTxnBuffer.remove(txnid);
        if (txn == null) {
            /*
             * feeder filler is unable to filter some commit/abort msg for
             * internal db or dup db. So it is possible we can see some
             * phantom commit/abort without an open txn in buffer. But we
             * wont receive PUT/DEL for such internal db entries, so there
             * is no open txn for such commit/abort in buffer.
             */
            logger.finest(() -> lm("Abort a non-existent txnid=" + txnid + "," +
                                   " ignore."));
            return;
        }

        if (includeAbortTxn) {
            final long ts = entry.getLastUpdateMs();
            final long abortVLSN = entry.getVLSN();
            commitAbortHelper(false, txn, txnid, ts, abortVLSN);
        }

        /* remove txn from openTxnBuffer and update openTxnBuffer stats */
        final long numOps =  txn.size();
        numAbortOps.addAndGet(numOps);
        numAbortTxn.getAndIncrement();
        lastAbortVLSN = entry.getVLSN();
        logger.fine(() -> lm("Aborted txn=" + txnid +
                             " with vlsn=" + lastAbortVLSN +
                             ", # of ops aborted=" + numOps));
    }

    /**
     * Returns true if the stream operation is from a table that streams
     * transactions, false otherwise
     */
    private boolean isStreamTxnTable(StreamOperation op) {
        final PublishingUnit pu = parent.getPu();
        if (pu == null) {
            /* some unit test only */
            return false;
        }
        final Table table;
        if (op.getType().equals(StreamOperation.Type.PUT)) {
            table = op.asPut().getRow().getTable();
        } else if (op.getType().equals(StreamOperation.Type.DELETE)) {
            table = op.asDelete().getPrimaryKey().getTable();
        } else {
            /* not put or delete */
            return false;
        }

        /* gets its top table name to decide if stream txn */
        final String topTableName =
            ((TableImpl) table).getTopLevelTable().getFullNamespaceName();
        return pu.getStreamTransaction(topTableName);
    }

    /* Commits an open txn from openTxnBuffer  */
    private synchronized void commit(final DataEntry entry)
        throws SubscriptionFailureException, InterruptedException {

        assert (DataEntry.Type.TXN_COMMIT.equals(entry.getType()));

        final long txnid = entry.getTxnID();
        final long vlsn = entry.getVLSN();
        final List<DataEntry> txn = openTxnBuffer.remove(txnid);
        if (txn == null) {
            /*
             * feeder filler is unable to filter some commit/abort msg for
             * internal db or dup db. So it is possible we can see some
             * phantom commit/abort without an open txn in buffer. But we
             * wont receive PUT/DEL for such internal db entries, so there
             * is no open txn for such commit/abort in buffer.
             */
            logger.finest(() -> "Ignore a non-existent txn id=" + txnid +
                                ", vlsn=" + vlsn);
            return;
        }

        commitAbortHelper(true, txn, txnid, entry.getLastUpdateMs(), vlsn);

        /* remove txn from openTxnBuffer and update openTxnBuffer stats */
        final long numOps =  txn.size();
        numCommitOps.addAndGet(numOps);
        numCommitTxn.getAndIncrement();
        lastCommitVLSN = vlsn;
        logger.finest(() -> lm("Committed txn=" + txnid + " with vlsn=" +
                               lastCommitVLSN + ", # of ops committed=" +
                               numOps));
    }

    /* Commits a transaction and convert data entry to stream operations */
    private void commitAbortHelper(boolean commit,
                                   List<DataEntry> allOps,
                                   long txnId,
                                   long ts,
                                   long commitVLSN)
        throws SubscriptionFailureException, InterruptedException {
        final List<StreamOperation> txnOps = new ArrayList<>();
        for (DataEntry entry : allOps) {

            /* sanity check just in case */
            final DataEntry.Type type = entry.getType();
            if ((type != DataEntry.Type.PUT) &&
                (type != DataEntry.Type.DELETE)) {
                throw new IllegalStateException(
                    "Type=" + type + " cannot be streamed to client.");
            }
            if (entry.getKey() == null) {
                throw new IllegalStateException(
                    "key cannot be null when being deserialized.");
            }

             /* process a partition generation db entry */
            if (partGenMarkProcessor.process(entry)) {
                continue;
            }

            /* regular data entry */
            final StreamOperation msg = buildStreamOp(entry.getKey(),
                                                      entry.getValue(),
                                                      type,
                                                      entry.getVLSN(),
                                                      entry.getLastUpdateMs(),
                                                      entry.getExpirationMs());
            if (msg == null) {
                /* cannot deserialize the entry */
                continue;
            }
            if (!isStreamTxnTable(msg)) {
                /* enqueue stream operation */
                enqueueMsg(msg);
                logger.finest(() -> lm("Enqueue " + (commit ?
                    "commit" : "abort") + " op=" + msg));
            } else {
                /* cache the ops in transaction */
                txnOps.add(msg);
            }
        }

        if (!txnOps.isEmpty()) {
            /* create txn stream operation */
            final int shardId = repGroupId.getGroupId();
            final TransactionIdImpl
                id = new TransactionIdImpl(shardId, txnId, ts);
            final StreamSequenceId sq = new StreamSequenceId(commitVLSN);
            final StreamOperation txn =
                new StreamTxnEvent(id, sq, commit, txnOps);
            /* enqueue stream operation */
            enqueueMsg(txn);
            logger.finest(() -> lm("Enqueue " + (commit ?
                "commit" : "abort") + " txn=" + txn));
        }
    }

    /* Enqueues a msg to output queue */
    private void enqueueMsg(StreamOperation msg) throws InterruptedException {
        if (msg == null) {
            return;
        }

        /* put the message in output queue */
        try {
            outputQueue.enqueue(msg,
                                () -> parent.getPu().isSubscriptionCanceled());
        } catch (InterruptedException e) {
            /* This might have to get smarter. */
            logger.warning(lm("Interrupted offering output queue, " +
                              "throw and let main loop to capture it and " +
                              "check shutdown"));
            throw e;
        }
    }

    /**
     * Gets a map of tables for the key.  The list contains all subscribed
     * tables with the key's root table ID if streaming specific tables, or
     * just the root table if streaming all tables.
     */
    private Map<String, TableImpl> getCachedTables(byte[] key) {
        final String rootTableId = getRootTableIdString(key);

        /* Check for valid table key */
        if (rootTableId == null) {
            logger.fine(() -> lm("Unable to get root table id from" +
                                 " key=" + Arrays.toString(key)));
            return null;
        }
        /* get a sync map of tables */
        final Map<String, TableImpl> tbList = cachedTables.get(rootTableId);

        if (!streamAllTables) {
            return tbList;
        }

        /* if stream all tables */
        if (tbList != null) {
            /* good, have cached copy */
            return tbList;
        }

        /*
         * Root table was dropped, won't stream any op for this table or its
         * children
         */
        if (droppedTables.contains(rootTableId)) {
            return null;
        }

        /* A root table id we see for the first time */
        final TableImpl table;
        try {
            table = parent.getPu().getTableMDManager()
                          .getTable(rootTableId,
                                    parent.getPu().isUseTblNameAsId());
        } catch (IllegalArgumentException | IndexOutOfBoundsException exp) {
            /* Invalid UTF bytes or invalid table ID, ignore */
            logger.fine(() -> "Invalid table id string=" + rootTableId);
            return null;
        }

        if (table == null) {
            /* already dropped, a short lived table */
            droppedTables.add(rootTableId);
            return null;
        }

        /* add the root table */
        addCachedTable(rootTableId, table);

        return cachedTables.get(rootTableId);
    }

    /* Builds stream operation */
    private StreamOperation buildStreamOp(byte[] key,
                                          byte[] value,
                                          DataEntry.Type type,
                                          long vlsn,
                                          long lastUpdateMs,
                                          long expirationMs)
        throws SubscriptionFailureException {

        /* check if the key belongs to a subscribed table */
        final Map<String, TableImpl> tbMap = getCachedTables(key);
        if (tbMap == null) {
            /* not a subscribed table or dropped */
            logger.finest(() -> lm("Key not found in subscribed tables" +
                                   ", cached tables=" + cachedTables.keySet() +
                                   ", dropped tables="+ droppedTables));
            return null;
        }

        /* this key is from a subscribed table, deserialize it */
        synchronized (tbMap) {
            for (TableImpl t : tbMap.values()) {
                final StreamOperation op =
                    deserialize(t, key, value, type, vlsn, lastUpdateMs,
                                expirationMs);
                if (op != null) {
                    return op;
                }
            }
        }

        //TODO: various reasons that a row/key cannot deserialize, some
        // are ok but other might indicate an error:
        // - row is from a child table that is not subscribed;
        // - table is removed from the stream but filter is not
        // updated yet so it still sees the writes from the table;
        // - the row is from an unexpected table;
        // The last reason indicates an error in filter and should be
        // dumped as warning. But it is not straightforward to
        // distinguish the last reason from others, need to revisit
        // later

        /* cannot deserialize */
        logger.finest(() -> lm("Cannot deserialize key from tables=" +
                               tbMap.keySet()));
        return null;
    }

    /* deserialize row from given table, null if cannot deserialized */
    private StreamOperation deserialize(TableImpl table,
                                        byte[] key,
                                        byte[] value,
                                        DataEntry.Type type,
                                        long vlsn,
                                        long lastUpdateMs,
                                        long expirationMs) {
        try {
            return createMsg(table, key, value, type, vlsn, lastUpdateMs,
                             expirationMs);
        } catch (SubscribedTableVersionException stve) {

            final String rootTableId = getTableId(table.getTopLevelTable());
            final String tableId = getTableId(table);
            /*
             * Ask table md manager in publisher to refresh, if success,
             * make a local copy and replace the old one. If null, it means
             * table has been dropped. If fail to refresh, SFE will be raised
             * from table md manager and propagated to caller.
             */
            final TableImpl refresh = parent.getPu()
                                            .getTableMDManager()
                                            .refreshTable(
                                                stve.getSubscriberId(),
                                                stve.getTable(),
                                                stve.getRequiredVersion(),
                                                tableId);
            if (refresh != null) {
                replaceCachedTable(rootTableId, refresh);
                logger.info(lm("Subscriber=" + stve.getSubscriberId()  +
                               ", shard=" + stve.getRepGroupId() +
                               " refreshed table=" +
                               table.getFullNamespaceName() +
                               " from ver=" + stve.getCurrentVer() +
                               " to ver=" + stve.getRequiredVersion()));
                /* we should not fail this time! */
                return createMsg(refresh, key, value, type, vlsn,
                                 lastUpdateMs, expirationMs);
            }

            final String err =
                "Subscriber=" + stve.getSubscriberId()  +
                " is unable to refreshed table=" +
                table.getFullNamespaceName() +
                " from ver=" + stve.getCurrentVer() +
                " to ver= " + stve.getRequiredVersion() +
                ", the table is dropped at kvstore, and no ops from that " +
                "table will be streamed from now on.";

            removeCachedTable(rootTableId, tableId);
            droppedTables.add(tableId);

            /* signal user that table is dropped */
            try {
                parent.getPu().getSubscriber()
                      .onWarn(new IllegalArgumentException(err));

            } catch (Exception exp) {
                logger.warning(lm("Exception in executing subscriber's " +
                                  "onWarn()=" + exp + "\n" +
                                  LoggerUtils.getStackTrace(exp)));
            }
            logger.warning(lm(err));
            return null;
        }
    }

    /* Creates a message to be consumed by client */
    private StreamOperation createMsg(TableImpl table,
                                      byte[] key,
                                      byte[] value,
                                      DataEntry.Type type,
                                      long vlsn,
                                      long lastUpdateMs,
                                      long expirationMs)
        throws SubscribedTableVersionException {

        final StreamSequenceId sequenceId = new StreamSequenceId(vlsn);
        NoSQLSubscriberId subscriberId = (parent.getPu() == null) ? null :
                                         parent.getPu().getSubscriberId();

        switch (type) {
            case PUT:
                /*
                 * Get the target table from the root table when streaming all
                 * tables since row deserialization requires the exact table,
                 * which may be a child.
                 */
                if (streamAllTables) {
                    table = table.findTargetTable(key);
                }
                return deserializer.getPutEvent(
                    subscriberId, repGroupId, table, key, value, sequenceId,
                    lastUpdateMs, expirationMs);

            case DELETE:
                return deserializer.getDeleteEvent(
                    subscriberId, repGroupId, table, key, value, sequenceId,
                    lastUpdateMs, expirationMs, !streamAllTables);
            default:
                /* should never reach here */
                throw new AssertionError("Unrecognized type " + type);
        }
    }

    private String lm(String msg) {
        return "[OTB-" + parent.getConsumerId() + "] " + msg;
    }

    /*
     * Object that processes each entry from replication stream. If the entry
     * is a put or delete, the worker thread will just post it in the open txn
     * buffer, or creates a new open txn. If the entry is a commit, the worker
     * will close the open txn in buffer and send a list of ops to output
     * queue; if the entry is an abort, the worker will just close the txn and
     * forget about it.
     *
     * The worker thread is a thread running in parallel with the thread
     * running as a subscription client, both working together to create a
     * pipelined processing mode.
     */
    private class TxnWorkerThread extends StoppableThread {

        TxnWorkerThread(String workerId) {
            super("TxnWorkerThread-" + workerId);
        }

        @Override
        public void run() {

            logger.fine(lm("Txn worker thread starts."));
            Throwable reasonOfExit = null;
            try {
                while (!isShutdown()) {

                    final DataEntry entry = inputQueue.poll(
                        INPUT_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (entry == null) {
                        logger.finest(() -> lm(
                            "Unable to dequeue for time(secs)=" +
                            (INPUT_QUEUE_POLL_TIMEOUT_MS / 1000)));
                        continue;
                    }

                    assert ExceptionTestHookExecute.doHookIfSet(expHook, entry);

                    final DataEntry.Type type = entry.getType();
                    switch (type) {
                        case TXN_ABORT:
                            abort(entry);
                            break;
                        case TXN_COMMIT:
                            commit(entry);
                            break;
                        case PUT:
                        case DELETE:
                            addEntry(entry);
                            break;
                        default:
                            throw new AssertionError(type);
                    }
                }
            } catch (SubscriptionFailureException sfe) {
                /* record and will deliver to user */
                reasonOfExit = sfe;
                logger.warning(lm("Txn worker thread has to close pu " +
                                  "because of subscription failure= " + sfe +
                                  ", cause=" + sfe.getCause()));
            } catch (InterruptedException ie) {
                if (isShutdown()) {
                    logger.fine(() -> lm("Txn worker thread would exit in " +
                                         "shutdown"));
                } else {
                    /* unexpected IE, record and will deliver to user */
                    reasonOfExit = ie;
                    logger.warning(lm("Txn worker thread interrupted" +
                                      ", error=" + ie));
                }
            } catch (Throwable exp) {
                /* all other exceptions/errors, record and deliver to user */
                reasonOfExit = exp;
            } finally {
                /*
                 * The txn worker thread is a long-running thread with same
                 * lifespan of its parent replication stream consumer. Unless
                 * it is shutdown normally, exit of the thread should
                 * terminate the complete stream and signal subscriber
                 */
                if (isShutdown() || reasonOfExit == null) {
                    logger.info(lm("Txn worker thread exits, shutdown=" +
                                   isShutdown()));
                } else {
                    logger.warning(lm("Txn worker thread exits with" +
                                      " reason=" + reasonOfExit +
                                      ", shutting down stream, stack=\n" +
                                      LoggerUtils.getStackTrace(reasonOfExit)));
                    /* shut down the stream and signal onError() */
                    parent.getPu().close(reasonOfExit);
                }
            }
        }

        @Override
        protected int initiateSoftShutdown() {
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(() -> lm("Signal to txn worker to shutdown, " +
                                 "already signalled?=" + alreadySet +
                                 ", wait for time(ms)=" +
                                 INPUT_QUEUE_POLL_TIMEOUT_MS +
                                 " to let it exit"));
            return INPUT_QUEUE_POLL_TIMEOUT_MS;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    private String getTableId(TableImpl t) {
        return PublishingUnit.getTableId(parent.getPu(), t);
    }

    /* gets the root table id from key, return null if no valid root id */
    private static String getRootTableIdString(byte[] key) {
        final Key.BinaryKeyIterator keyIter = new Key.BinaryKeyIterator(key);
        if (keyIter.atEndOfKey()) {
            return null;
        }

        try {
            return keyIter.next();
        } catch (RuntimeException exp) {
            /* unable to get a valid root table id from key */
            return null;
        }
    }

    public static class DefaultDeserializer
        implements StreamOperation.Deserializer {

        @Override
        public PutEvent getPutEvent(NoSQLSubscriberId subscriberId,
                                    RepGroupId rgId,
                                    TableImpl table,
                                    byte[] key,
                                    byte[] value,
                                    SequenceId sequenceId,
                                    long lastModificationTime,
                                    long expirationTime) {
            RowImpl row;
            try {
                /* Deserialize complete row */
                row = table.createRowFromBytes(
                    key, value, table.isKeyOnly(),
                    false/* do not add missing col */);

            } catch (TableVersionException tve) {
                /* need refresh table md */
                throw new SubscribedTableVersionException(
                    subscriberId, rgId,
                    table.getFullNamespaceName(),
                    tve.getRequiredVersion(),
                    table.getTableVersion());
            }
            /* key was not associated with a table */
            if (row == null) {
                return null;
            }
            /* set last update time */
            row.setModificationTime(lastModificationTime);
            /* set expiration time */
            row.setExpirationTime(expirationTime);
            /* populate size info */
            row.setStorageSize(key.length + (value == null ? 0 : value.length));
            return new StreamPutEvent(row, sequenceId, rgId.getGroupId());
        }

        @Override
        public DeleteEvent getDeleteEvent(NoSQLSubscriberId subscriberId,
                                          RepGroupId rgId,
                                          TableImpl table,
                                          byte[] key,
                                          byte[] value,
                                          SequenceId sequenceId,
                                          long lastModificationTime,
                                          long expirationTime,
                                          boolean exactTable) {
            PrimaryKeyImpl delKey;
            try {
                /* a primary key */
                delKey = table.createPrimaryKeyFromKeyBytes(key);
            } catch (TableVersionException tve) {
                /* need refresh table md */
                throw new SubscribedTableVersionException(
                    subscriberId, rgId,
                    table.getFullNamespaceName(),
                    tve.getRequiredVersion(),
                    table.getTableVersion());
            }

            /* key was not associated with a table */
            if (delKey == null) {
                return null;
            }
            /* set last update time */
            delKey.setModificationTime(lastModificationTime);

            /*
             * if a tombstone delete and tombstone is in MR format, set the
             * region id to primary key.
             */
            if (value != null && value.length > 0) {
                int regionId = PackedInteger.readInt(value, 1);
                delKey.setRegionId(regionId);
            }

            /*
             * The parent table can deserialize the primary key from a
             * child table, so need to check if table matches when not
             * streaming all tables ?
             */
            if (exactTable && !table.getFullNamespaceName().equals(
                    delKey.getTable().getFullNamespaceName())) {
                return null;
            }
            /* populate size info */
            delKey.setStorageSize(key.length +
                                  (value == null ? 0 : value.length));
            return new StreamDelEvent(delKey, sequenceId, rgId.getGroupId());
        }
    }

    /**
     * A dummy stream operation that represents a closed partition generation
     * record.
     */
    public static class ClosedPartGenStreamOp implements StreamOperation {

        /** shard id of the partition generation record */
        private final int shardId;
        /** sequence id of the dummy stream operation */
        private final SequenceId sequenceId;
        /** embedded closed partition generation */
        private final PartitionGeneration closeGen;

        ClosedPartGenStreamOp(int shardId,
                              long vlsn,
                              PartitionGeneration closeGen) {
            if (closeGen.isOpen()) {
                throw new IllegalArgumentException("Generation is open");
            }
            this.closeGen = closeGen;
            this.shardId = shardId;
            sequenceId = new StreamSequenceId(vlsn);
        }

        PartitionGeneration getCloseGen() {
            return closeGen;
        }

        @Override
        public SequenceId getSequenceId() {
            return sequenceId;
        }

        @Override
        public int getRepGroupId() {
            return shardId;
        }

        @Override
        public long getTableId() {
            throw getUnsupportedException();
        }

        @Override
        public String getFullTableName() {
            throw getUnsupportedException();
        }

        @Override
        public String getTableName() {
            throw getUnsupportedException();
        }

        @Override
        public int getRegionId() {
            throw getUnsupportedException();
        }

        @Override
        public long getLastModificationTime() {
            throw getUnsupportedException();
        }

        @Override
        public long getExpirationTime() {
            throw getUnsupportedException();
        }

        @Override
        public String toJsonString() {
            throw getUnsupportedException();
        }

        @Override
        public Type getType() {
            return Type.INTERNAL;
        }

        @Override
        public PutEvent asPut() {
            throw getUnsupportedException();
        }

        @Override
        public DeleteEvent asDelete() {
            throw getUnsupportedException();
        }

        private UnsupportedOperationException getUnsupportedException() {
            return new UnsupportedOperationException(
                "Unsupported operation, shard=" + shardId +
                ", sequence id=" +
                ((StreamSequenceId) sequenceId).getSequence() +
                ", closed generation=" + closeGen);
        }
    }
}
