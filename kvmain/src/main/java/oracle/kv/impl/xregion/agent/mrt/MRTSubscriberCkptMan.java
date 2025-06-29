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

package oracle.kv.impl.xregion.agent.mrt;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.utilint.StoppableThread;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Object to manage the stream checkpoint in {@link MRTSubscriber}
 */
public class MRTSubscriberCkptMan extends StoppableThread {

    /** A test hook that can be used by test to verify checkpoint */
    public static volatile TestHook<StreamPosition> ckptTestHook= null;

    /** wait time in ms during soft shutdown, give enough time for last ckpt */
    private static final int SOFT_SHUTDOWN_WAIT_MS = 10 * 1000;
    /** max queued checkpoint candidates */
    private static final int MAX_CHECKPOINT_CANDIDATES = 1024;
    /** queue polling timeout in ms */
    private static final long QUEUE_TIMEOUT_MS = 1000;
    /** wait in ms if checkpoint not ready to commit */
    private static final int CKPT_WAIT_TIMEOUT_MS = 1000;
    /** rate limiting log period in ms */
    private static final int RL_LOG_PERIOD_MS = 60 * 1000;
    /** checkpoint table name space */
    private static final String CKPT_TABLE_NAMESPACE =
        TableAPI.SYSDEFAULT_NAMESPACE_NAME;
    /** prefix of checkpoint table name */
    private static final String CKPT_TABLE_NAME_PREFIX = "RegionAgentCkpt";

    /**
     * Map from the array index of a queue in {@link
     * MRTSubscriber#pendingOperations} to a stream position representing the
     * highest position in each shard for all persisted operations from that
     * queue. The high water mark is updated when an pending operation is
     * complete successfully, and thus it never retreats.
     * <p>
     * The per-queue high water marks will be combined by using the lowest
     * position over all queues for each shard. That combined stream position
     * will be used to determine if the checkpoint can be safely made.
     */
    private final ConcurrentHashMap<Integer, StreamPosition> highWaterMark;

    /**
     * Queue of stream positions waiting to be committed. The checkpoint
     * stream position is generated by the parent {@link MRTSubscriber} based
     * on the stream position when a stream operation is received and on the
     * checkpoint interval parameter. The stream position will be saved after
     * the associated operations have been persisted.
     * <p>
     * This is a limited size queue. If the queue fills up, attempting to add
     * a new checkpoint will block and that blocking will cause the caller
     * {@link MRTSubscriber#onNext} to block. On the other hand, if {@link
     * MRTSubscriber#pendingOperations} fills up,
     * {@link MRTSubscriber#onNext} will be blocked as well and it cannot
     * enqueue new stream positions.
     */
    private final BlockingQueue<StreamPosition> ckptQueue;

    /** parent subscriber */
    private final MRTSubscriber parent;
    /** metadata management */
    private final ServiceMDMan mdMan;
    /** parent subscriber id */
    private final NoSQLSubscriberId id;
    /** source region info */
    private final RegionInfo souceRegionInfo;
    /** true if shut down the agent */
    private volatile boolean shutdownRequested;
    /** ckpt table interval in seconds */
    private final long ckptIntvSecs;
    /** ckpt table interval in # stream ops */
    private final long ckptIntvNumOps;
    /** last checkpoint timestamp in ms */
    private volatile long lastCkptTimeMs;
    /** # ops streamed at last checkpoint */
    private volatile long lastCkptStreamedOps;
    /** source store name */
    private final String srcStoreName;
    /** source store id */
    private final long srcStoreId;
    /** private logger */
    private final Logger logger;
    /** rate limiting logger */
    private final RateLimitingLogger<String> rlLogger;

    /** used by unit test only */
    private volatile StreamPosition lastPerformedCkpt = null;
    private final AtomicLong numCkptPerformed = new AtomicLong(0);

    public MRTSubscriberCkptMan(MRTSubscriber parent,
                                NoSQLSubscriberId id,
                                RegionInfo souceRegionInfo,
                                long ckptIntvNumOps,
                                long ckptIntvSecs,
                                ServiceMDMan mdMan,
                                Logger logger) {
        super("MRTSubscriberCkptMan-" + souceRegionInfo.getName() + "-" + id);
        this.parent = parent;
        this.id = id;
        this.souceRegionInfo = souceRegionInfo;
        this.ckptIntvNumOps = ckptIntvNumOps;
        this.ckptIntvSecs = ckptIntvSecs;
        this.mdMan = mdMan;
        this.logger = logger;

        rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS, 1024, logger);
        shutdownRequested = false;
        ckptQueue = new ArrayBlockingQueue<>(MAX_CHECKPOINT_CANDIDATES);
        highWaterMark = new ConcurrentHashMap<>();
        lastCkptStreamedOps = 0;
        lastCkptTimeMs = 0; /* to be initialized when thread starts up */

        final KVStoreImpl store = getSourceStoreKVS();
        srcStoreName = store.getTopology().getKVStoreName();
        srcStoreId = store.getTopology().getId();
    }

    /* Public APIs */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected int initiateSoftShutdown() {
        return SOFT_SHUTDOWN_WAIT_MS;
    }

    @Override
    public void run() {

        logger.info(lm("Checkpoint manager starts up" +
                       ", source store=" + srcStoreName +
                       "(id=" + srcStoreId + ")" +
                       ", ckpt interval secs=" + ckptIntvSecs +
                       ", ckpt interval #ops=" + ckptIntvNumOps +
                       ", commit wait timeout ms=" + CKPT_WAIT_TIMEOUT_MS +
                       ", queue timeout ms=" + QUEUE_TIMEOUT_MS));
        lastCkptTimeMs = System.currentTimeMillis();
        try {
            while (!shutdownRequested) {
                final StreamPosition ckpt =
                    ckptQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (ckpt == null) {
                    final String msg = "No checkpoint candidate, retry";
                    rlLogger.log(msg, Level.FINE, () -> lm(msg));
                    continue;
                }

                /* wait for the high water mark to grow */
                StreamPosition minHWM = null;
                final String msg = "Cannot checkpoint=" + ckpt +
                                   ", wait till notified with timeout ms=" +
                                   CKPT_WAIT_TIMEOUT_MS;
                while (!shutdownRequested) {
                    minHWM = getGlobalMinHWM();
                    if (readyToCommit(ckpt, minHWM)) {
                        break;
                    }

                    /* not ready to commit, wait */
                    rlLogger.log(msg, Level.FINE, () -> lm(msg));
                    synchronized (this) {
                        wait(CKPT_WAIT_TIMEOUT_MS);
                        /* if shut down, will exit on while */
                    }
                }

                if (shutdownRequested) {
                    logger.fine(() -> lm("Shut down, skip checkpoint=" + ckpt));
                    break;
                }

                /* persist the current minimal high water mark */
                maybeDoCheckpoint(minHWM);
            }
        } catch (InterruptedException ie) {
            logger.warning(lm("Interrupted in sleeping, will exit"));
        } catch (Exception exp) {
            logger.warning(lm("Shut down checkpoint manager due to" +
                              " error=" + exp + "\n" +
                              LoggerUtils.getStackTrace(exp)));
        } finally {
            final long count = (parent == null /* unit test */) ?
                numCkptPerformed.get() :
                parent.getMetrics().getNumCheckpoints();
            logger.info(lm("Checkpoint manager exits, # performed " +
                           "checkpoints=" + count + ", # of queued" +
                           "checkpoints=" + ckptQueue.size()));
        }
    }

    /**
     * Stops the agent thread from another thread without error
     */
    public void shutdown() {
        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;
        final long ts = System.currentTimeMillis();
        synchronized (this) {
            notifyAll();
        }
        shutdownThread(logger);
        logger.info(lm("Checkpoint manager has shut down in time(ms)=" +
                       (System.currentTimeMillis() - ts)));
    }

    /**
     * Queues a checkpoint if necessary
     */
    void queueCkptIfNeeded() {
        if (!needCkpt()) {
            return;
        }
        final NoSQLSubscription stream = parent.getSubscription();
        if (shutdownRequested) {
            logger.finest(() -> lm("Shut down, skip queue checkpoint"));
            return;
        }
        queueCkptCandidate(stream.getCurrentPosition());
    }

    /**
     * Queues a given checkpoint candidate
     */
    public void queueCkptCandidate(StreamPosition ckpt) {
        try {
            while (!ckptQueue.offer(ckpt, QUEUE_TIMEOUT_MS,
                                    TimeUnit.MILLISECONDS)) {
                if (shutdownRequested) {
                    logger.finest(() -> lm("In shutdown, skip enqueue " +
                                           "checkpoint candidate=" + ckpt));
                    return;
                }
            }
        } catch (InterruptedException e) {
            final String err = "Interrupted in offering checkpoint=" + ckpt;
            logger.warning(lm(err));
            /* surface and let stream shut down */
            throw new IllegalStateException(err);
        }
        logger.info(lm("Generate checkpoint=" + ckpt));
    }

    /**
     * Returns the global minimal high water mark of all queues
     * @return global minimal high water mark
     */
    public StreamPosition getGlobalMinHWM() {
        final StreamPosition ret = new StreamPosition(srcStoreName, srcStoreId);
        highWaterMark.values().forEach(pos -> merge(ret, pos));
        return ret;
    }

    /** Gets kvstore handle of source store */
    public KVStoreImpl getSourceStoreKVS() {
        return (KVStoreImpl) mdMan.getRegionKVS(souceRegionInfo);
    }

    /**
     * Updates the high water mark for complete operation
     * @param queueId  queue id
     * @param gid      shard id
     * @param vlsn     vlsn
     */
    public void updateHWM(int queueId, int gid, long vlsn) {
        final StreamPosition streamPos =
            highWaterMark.computeIfAbsent(
                queueId, u -> new StreamPosition(srcStoreName, srcStoreId));
        streamPos.setShardPosition(gid, vlsn);
    }

    /* private functions */
    private String lm(String msg) {
        return "[MRTSubscriberCkptMan-" +
               souceRegionInfo.getName() + "-" + id + "] " + msg;
    }

    /**
     * Returns true if it is safe to checkpoint the given stream position
     * @param pos give stream position
     * @param minHWM minimal high water mark
     * @return true if OK to checkpoint, false otherwise
     */
    private boolean readyToCommit(StreamPosition pos, StreamPosition minHWM) {
        if (!srcStoreName.equals(pos.getStoreName())) {
            throw new IllegalArgumentException(
                "Store name=" + pos.getStoreName() + " in checkpoint does not" +
                " match the source region store name=" + srcStoreName);
        }
        if (srcStoreId != pos.getStoreId()) {
            throw new IllegalArgumentException(
                "Store id=" + pos.getStoreId() + " in checkpoint does not" +
                " match the source region store id=" + srcStoreId);
        }

        if (!equalOrHigher(minHWM, pos)) {
            logger.finest(() -> lm("Not ready to perform checkpoint=" + minHWM +
                                   " because it has not surpassed " +
                                   "requested=" + pos));
            return false;
        }

        logger.info(lm("OK to perform checkpoint=" + minHWM +
                       " because it has surpassed requested=" + pos));
        return true;
    }

    /**
     * Returns true if the high water mark position has been equal to or
     * higher than the stream position
     * @param hwm  high water mark
     * @param pos  stream position
     * @return true if high water mark position has been equal to or higher
     * than the stream position
     */
    private boolean equalOrHigher(@NonNull StreamPosition hwm,
                                  @NonNull StreamPosition pos) {
        for (StreamPosition.ShardPosition shardPos : pos.getAllShardPos()) {
            final int gid = shardPos.getRepGroupId();
            final StreamPosition.ShardPosition min = hwm.getShardPosition(gid);
            if (min == null) {
                rlLogger.log("shard=" + gid + pos, Level.FINE,
                             () -> lm("Cannot checkpoint because of " +
                                      "unavailable shard=" + gid + " in " +
                                      "minimal high water mark=" + hwm +
                                      ", requested=" + pos));
                return false;
            }
            if (min.getVLSN() < shardPos.getVLSN()) {
                rlLogger.log("shard=" + gid + pos, Level.FINE,
                             () -> lm("Cannot checkpoint because in minimal " +
                                      "high water mark, shard=" + gid +
                                      " vlsn=" + min.getVLSN() +  " lower " +
                                      "than ckpt vlsn=" + shardPos.getVLSN()));
                return false;
            }
        }
        return true;
    }

    /**
     * Merge given position with the current stream position, by modifying curr
     * to use any smaller VLSN of each shard in pos.
     *
     * @param curr current stream position
     * @param pos  given stream position
     */
    private void merge(StreamPosition curr, StreamPosition pos) {
        pos.getAllShardPos().forEach(shardPos -> {
            final int gid = shardPos.getRepGroupId();
            final long vlsn = shardPos.getVLSN();
            final StreamPosition.ShardPosition sp = curr.getShardPosition(gid);
            if (sp == null || vlsn < sp.getVLSN()) {
                curr.setShardPosition(gid, vlsn);
            }
        });
    }

    /**
     * Commits the checkpoint
     * @param ckpt stream position to checkpoint
     */
    private void maybeDoCheckpoint(StreamPosition ckpt) {
        if (ckpt == null) {
            return;
        }
        logger.finest(() -> lm("Performing checkpoint=" + ckpt));
        numCkptPerformed.addAndGet(1);
        if (parent == null) {
            /* unit test only */
            lastPerformedCkpt = new StreamPosition(ckpt);
            return;
        }

        final NoSQLSubscription stream = parent.getSubscription();
        if (stream.isCanceled()) {
            logger.finest(() -> lm("Subscription canceled, " +
                                   "skip checkpoint=" + ckpt));
            return;
        }
        stream.doCheckpoint(ckpt, true);

        /* unit test only */
        assert TestHookExecute.doHookIfSet(ckptTestHook, ckpt);
    }

    /**
     * Returns true if checkpoint is needed, false otherwise
     */
    private boolean needCkpt() {
        return enoughTimeCkpt() || enoughOpsCkpt();
    }

    /**
     * Returns true if enough time has elapsed for checkpoint
     */
    private boolean enoughTimeCkpt() {
        final long now = System.currentTimeMillis();
        final boolean succ = ckptIntvSecs <= ((now - lastCkptTimeMs) / 1000);
        if (succ) {
            logger.fine(() -> lm("Elapsed enough time to checkpoint" +
                                 "[lastMs=" + lastCkptTimeMs +
                                 ", currMs=" + now +
                                 ", elapsedSec=" +
                                 ((now - lastCkptTimeMs) / 1000) +
                                 ", intvSec=" + ckptIntvSecs + "]"));
            /* reset stats */
            lastCkptTimeMs = now;
            lastCkptStreamedOps = parent.getMetrics().getTotalStreamOps();
        }
        return succ;
    }

    /**
     * Returns true if enough number of streamed ops has been received for
     * checkpoint
     */
    private boolean enoughOpsCkpt() {
        final long ops = parent.getMetrics().getTotalStreamOps();
        final boolean succ = ckptIntvNumOps <= (ops - lastCkptStreamedOps);
        if (succ) {
            logger.fine(() -> lm("Received enough ops to checkpoint" +
                                 "[#last=" + lastCkptStreamedOps +
                                 ", #curr=" + ops +
                                 ", #diff=" + (ops - lastCkptStreamedOps) +
                                 ", intvOps=" + ckptIntvNumOps + "]"));
            /* reset stats */
            lastCkptTimeMs = System.currentTimeMillis();
            lastCkptStreamedOps = ops;
        }
        return succ;
    }

    /**
     * Unit test only
     */
    public long getNumCkptsPerformed() {
        return numCkptPerformed.get();
    }

    /**
     * Unit test only
     */
    public long getNumCkptsQueued() {
        return ckptQueue.size();
    }

    /**
     * Unit test only
     * Gets last performed checkpoint
     */
    public StreamPosition getLastPerfCkpt() {
        return lastPerformedCkpt;
    }

    /**
     * Deletes all stale checkpoint tables after all agents in the group have
     * posted new checkpoints. Keep the stale checkpoint tables if any agent
     * has not yet posted its new checkpoint. Note the stale checkpoint
     * tables are those checkpoint tables created by the agents in the same
     * local region before group size change. These checkpoint tables are used
     * by agents in new group to build start stream position, and shall be
     * removed after all agents in the new group have created their own
     * checkpoint tables.
     */
    public void dropStaleCkptTableIfPossible() {

        if (id.getIndex() > 0) {
            logger.fine(() -> lm("Only the first agent in the group can " +
                                 "remove stale checkpoint tables, id=" + id));
            return;
        }

        /* get all checkpoint tables from my region */
        final Set<Table> localRegionCkpts = getLocalCkptTables();
        /* get all checkpoint tables made by my current group */
        final Set<Table> groupCkpts = getGroupCkptTables(localRegionCkpts);
        final int exp = id.getTotal();
        final int act = groupCkpts.size();
        if (act < exp) {
            /*
             * not all my group agents have made checkpoint table, keep
             * stale checkpoint tables for next check
             */
            logger.info(lm("Incomplete group checkpoint tables=" +
                           ServiceMDMan.getTrace(groupCkpts) +
                           ", # checkpoint tables=" + act +
                           ", group size=" + exp));
            return;
        }

        /*
         * delete stale checkpoints that are no longer needed, the stale
         * checkpoint tables are tables made by my region but not by members
         * in current group
         */
        final Set<Table> staleCkpts =
            localRegionCkpts.stream()
                            .filter(t -> !groupCkpts.contains(t))
                            .collect(Collectors.toSet());
        if (staleCkpts.isEmpty()) {
            logger.info(lm("No stale checkpoint table exist"));
            return;
        }

        final Set<String> dropped = new HashSet<>();
        final Set<String> failed = new HashSet<>();
        staleCkpts.stream()
                  .map(Table::getFullNamespaceName)
                  .forEach(t -> {
                      if (mdMan.dropTable(souceRegionInfo, t)) {
                          dropped.add(t);
                          logger.fine(() -> lm(
                              "Dropped stale checkpoint table=" + t));
                      } else {
                          /* to be dropped next time doing checkpoint */
                          failed.add(t);
                      }
                  });
        logger.info(lm("Removed stale checkpoint tables=" + dropped +
                       ", fail to remove tables=" + failed +
                       ", will try next time"));
    }

    /**
     * Unit test only
     * @return the stream checkpoint table or null if stream is not created.
     */
    public String getStreamCkptTableName() {
        final NoSQLSubscriptionImpl stream =
            (NoSQLSubscriptionImpl) parent.getSubscription();
        if (stream == null) {
            return null;
        }
        return stream.getCKptManager().getCkptTableName();
    }

    /**
     * Returns set of checkpoint tables made by local region
     * @return set of checkpoint tables made by local region, or an empty set
     * if not existing
     */
    public Set<Table> getLocalCkptTables() {
        final KVStore kvs = mdMan.getRegionKVS(souceRegionInfo);
        /* pull set of all stream checkpoint tables from source region */
        final Set<Table> allTbs = getAllCkptTables(kvs);
        /*
         * filter out all checkpoint tables not made by local region. For
         * example, if there are 3 regions, A, B, C and my region is A; on
         * the remote source region B, there could be stream checkpoint
         * tables made by agent in region C.
         */
        final Set<Table> tbs =
            allTbs.stream()
                  .filter(t -> localRegionCkptTable(t.getFullNamespaceName()))
                  .collect(Collectors.toSet());
        logger.info(lm("List of checkpoint tables from local region=" +
                       ServiceMDMan.getTrace(tbs) +
                       ", all checkpoint tables=" +
                       ServiceMDMan.getTrace(allTbs)));
        return tbs;
    }

    /**
     * Returns a set of checkpoint tables made by peer agents in same group
     * from a given set of checkpoint tables, or empty set
     * @param tbs set of checkpoint tables
     * @return a set of checkpoint tables made by peer agents, or empty set
     */
    public Set<Table> getGroupCkptTables(Set<Table> tbs) {
        final Set<Table> peerCkpts =
            tbs.stream()
               .filter(t -> sameGroup(t.getFullNamespaceName()))
               .collect(Collectors.toSet());
        logger.info(lm("Checkpoint tables made by my group" +
                       ", agent id=" + id  +
                       ", checkpoint tables=" +
                       ServiceMDMan.getTrace(peerCkpts)));
        return peerCkpts;
    }

    /**
     * Returns true if the given table is from the same group, false otherwise
     */
    private boolean sameGroup(String table) {
        final String srcRegion = souceRegionInfo.getName();
        final String hostName = parent.getParent().getHost().getName();
        /* matching source region and target region name */
        final MRTSubscriberCkptMan.StreamCheckpointName name;
        try {
            name = new MRTSubscriberCkptMan.StreamCheckpointName(table);
        } catch (IllegalArgumentException iae) {
            logger.info(lm("Cannot parse the table name=" + table +
                           ", error=" + iae));
            return false;
        }
        return name.sameGroup(srcRegion, hostName, id);
    }

    /**
     * Gets all stream checkpoint tables from a give kvstore. Note the
     * checkpoint tables can be made by either agents own region or other
     * regions.
     *
     * @param kvs  kvstore handle of the region
     * @return all checkpoint tables, or empty set if no checkpoint table
     */
    public static Set<Table> getAllCkptTables(KVStore kvs) {
        final TableAPI tapi = kvs.getTableAPI();
        final Map<String, Table> allTbs =
            tapi.getTables(CKPT_TABLE_NAMESPACE);
        return allTbs.values().stream()
                     .filter(t -> t.getFullNamespaceName()
                                   .startsWith(CKPT_TABLE_NAME_PREFIX))
                     .collect(Collectors.toSet());
    }

    /**
     * Returns true if the checkpoint table is from an agent in local
     * region
     *
     * @param table checkpoint table name
     * @return  true if the checkpoint table is from an agent in local
     * region, or false otherwise
     */
    private boolean localRegionCkptTable(String table) {
        final String srcRegion = souceRegionInfo.getName();
        final String hostName = parent.getParent().getHost().getName();
        /* matching source region and target region name */
        final MRTSubscriberCkptMan.StreamCheckpointName name;
        try {
            name = new MRTSubscriberCkptMan.StreamCheckpointName(table);
        } catch (IllegalArgumentException iae) {
            logger.info(lm("Cannot parse the table name=" + table +
                           ", error=" + iae));
            return false;
        }
        return name.peerCkpt(srcRegion, hostName);
    }

    /**
     * Class to build and parse stream checkpoint name
     */
    public static class StreamCheckpointName {

        /**
         * Stream checkpoint separators
         */
        private static final String DELIMITER = "_";
        private static final String FROM_STR = "from";
        private static final String TO_STR = "to";
        /**
         * Number of elements in stream checkpoint name
         */
        private static final int NUM_ELEMENTS = 7;

        private final String source;
        private final String target;

        private final int total;
        private final int index;

        public StreamCheckpointName(String tableName) {
            final String name =
                NameUtils.getFullNameFromQualifiedName(tableName);
            final String[] split = name.split(DELIMITER);
            if (split.length != NUM_ELEMENTS) {
                throw new IllegalArgumentException(
                    "Invalid checkpoint table name=" + tableName +
                    ", split=" + Arrays.toString(split)+
                    ", #elements=" + split.length +
                    ", expected #elements=" + NUM_ELEMENTS);
            }

            if (CKPT_TABLE_NAME_PREFIX.equals(split[0]) &&
                FROM_STR.equals(split[1]) &&
                TO_STR.equals(split[3])) {
                source = split[2];
                target = split[4];
                total = Integer.parseInt(split[5]);
                index = Integer.parseInt(split[6]);
                return;
            }

            throw new IllegalArgumentException(
                "Invalid checkpoint table name=" + tableName +
                ", split=" + Arrays.toString(split));
        }

        public String getSource() {
            return source;
        }

        public String getTarget() {
            return target;
        }

        public int getTotal() {
            return total;
        }

        public int getIndex() {
            return index;
        }

        /**
         * Builds the checkpoint table name for the agent. The format of stream
         * checkpoint table would be
         * <p>
         * table name := <CKPT_PREFIX>_from_<source>_to_<target>_<sid>
         * and
         * <sid> := <number of total agents>_<agent index>
         *
         * @return the checkpoint table name
         */
        public static String buildCkptTableName(String source,
                                                String target,
                                                NoSQLSubscriberId sid) {
            final String name = CKPT_TABLE_NAME_PREFIX +
                                DELIMITER + FROM_STR +
                                DELIMITER + source +
                                DELIMITER + TO_STR +
                                DELIMITER + target +
                                DELIMITER + sid;
            return NameUtils.makeQualifiedName(CKPT_TABLE_NAMESPACE,
                                               name);
        }

        public boolean peerCkpt(String src, String tgt) {
            return source.equals(src) && target.equals(tgt);
        }

        boolean sameGroup(String src, String tgt, NoSQLSubscriberId sid) {
            return peerCkpt(src, tgt) && sid.getTotal() == total;
        }
    }
}
