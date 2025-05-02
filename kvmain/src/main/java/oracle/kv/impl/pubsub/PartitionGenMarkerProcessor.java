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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.rep.migration.generation.PartitionGenDBManager;
import oracle.kv.impl.rep.migration.generation.PartitionGenNum;
import oracle.kv.impl.rep.migration.generation.PartitionGeneration;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;

import com.sleepycat.je.utilint.VLSN;

/**
 * Object represents the module to process partition generation marker. It is
 * used by stream client open transaction buffer to process the streamed
 * entries from feeder that represents the beginning and end of a partition
 * generation.
 */
public class PartitionGenMarkerProcessor {

    /**
     * Map of test hooks to track partition generation in test, indexed by
     * subscriber id
     */
    private static final Map<NoSQLSubscriberId,
            TestHook<PartitionGeneration>> partGenHookMap =
        new ConcurrentHashMap<>();

    /** default wait time in ms for green light from another shard */
    private final static long POLL_TIME_OUT_MS = Long.MAX_VALUE;

    /** polling interval in ms */
    private final static int POLL_INTERVAL_MS = 5000;

    /** parent open txn buffer */
    private final OpenTransactionBuffer otb;

    /** subscriber id */
    private final NoSQLSubscriberId sid;

    /** private logger */
    private final Logger logger;

    /** poll time out in ms */
    private volatile long pollTimeOutMs;

    /** true if using external checkpoint */
    private final boolean useExternalCkpt;

    /** true if waiting for a checkpoint from another shard */
    private volatile boolean waitForCkpt;

    PartitionGenMarkerProcessor(OpenTransactionBuffer otb,
                                Logger logger) {
        this.otb = otb;
        this.logger = logger;
        waitForCkpt = false;

        /* unless in unit test, we should always use default timeout */
        pollTimeOutMs = POLL_TIME_OUT_MS;
        final PublishingUnit pu = otb.getParentRSC().getPu();
        if (pu == null) {
            /* unit test only */
            sid = null;
            useExternalCkpt = false;
        } else {
            final NoSQLSubscriber subscriber = pu.getSubscriber();
            sid = subscriber.getSubscriptionConfig().getSubscriberId();
            useExternalCkpt = subscriber.getSubscriptionConfig()
                                        .getUseExtCkptForElasticity();
        }
    }

    /**
     * Unit test only, set partition generation test hook
     */
    public static void setPartitionGenerationTestHook(
        NoSQLSubscriberId sid, TestHook<PartitionGeneration> hook) {
        partGenHookMap.put(sid, hook);
    }

    /**
     * Unit test only, clear all test hooks
     */
    public static void clearPartitionGenerationTestHook() {
        partGenHookMap.clear();
    }

    /**
     * Unit test only, returns poll interval in ms
     */
    public static long getPollIntervalMs() {
        return POLL_INTERVAL_MS;
    }

    /**
     * Processes an entry from partition generation db
     *
     * @param entry an entry from partition generation db
     *
     * @return true if the entry from partition generation db and processed,
     * false if the entry is not from partition generation db
     */
    boolean process(DataEntry entry) throws InterruptedException {
        if (!isFromPartMDDB(entry)) {
            return false;
        }

        final PartitionGeneration gen =
            PartitionGenDBManager.readPartGenFromVal(entry.getValue());
        /* pass the generation to test */
        assert TestHookExecute.doHookIfSet(partGenHookMap.get(sid), gen);
        if (gen.isOpen()) {
            processOpenGen(gen, entry.getVLSN());
            return true;
        }

        /* enqueue the close generation for later processing */
        try {
            logger.info(lm("To enqueue a closed generation" +
                           " with vlsn=" + entry.getVLSN() +
                           ", generation=" + gen));
            otb.enqueueClosedGen(gen, entry.getVLSN());
        } catch (InterruptedException ie) {
            /* interrupted */
            logger.warning(lm("Enqueue of close generation interrupted, " +
                              "gen=" + gen + ", vlsn=" + entry.getVLSN() +
                              ", error=" + ie));
            /* let caller deal with it */
            throw ie;
        }
        logger.info(lm("Enqueued a dummy stream operation on closed " +
                       "generation, partition=" + gen.getPartId() +
                       ", vlsn=" + entry.getVLSN()));
        return true;
    }

    /**
     * returns true if the entry is from partition generation db, false
     * otherwise.
     *
     * @param entry input entry
     *
     * @return true if the entry from partition generation db
     */
    private boolean isFromPartMDDB(DataEntry entry) {
        return entry.getDbId().equals(otb.getParentRSC()
                                         .getRSCStat()
                                         .getPartGenDBId());
    }

    /**
     * Processes the open generation. It may block the stream till its
     * previous generation has been completely streamed from other shard. It
     * is required to maintain the correct order of streamed events with
     * migration. For more background and technical details, please refer to
     * [#26662]
     *
     * @param gen the open generation
     * @param vlsn vlsn of the entry that represents the open generation
     *
     * @throws SubscriptionFailureException if interrupted or timeout during
     * wait for stream of previous generation to catch up
     */
    private void processOpenGen(PartitionGeneration gen, long vlsn)
        throws SubscriptionFailureException {

        assert (gen.isOpen());

        /*
         * Generation zero represent the very first generation when store is
         * created. Stream client can safely ignore such entries to open
         * generation zero, because there is no previous generation of
         * generation zero to wait for.
         */
        final PartitionId pid = gen.getPartId();
        if (gen.getGenNum().equals(PartitionGenNum.generationZero())) {
            /* no need to wait */
            if (pid.equals(PartitionGeneration.getInitDoneMarkerId())) {
                logger.info(lm("Received marker that initialization of " +
                               "partition generation database is done"));
                return;
            }
            logger.fine(() -> lm("Received generation zero for=" + pid));
            return;
        }

        logger.info(lm("Receive open generation=" + gen +
                       ", polling for checkpoint of shard=" +
                       gen.getPrevGenRepGroup() +
                       ", vlsn>=" + gen.getPrevGenEndVLSN() +
                       ", poll interval ms=" + POLL_INTERVAL_MS +
                       ", poll timeout ms=" + pollTimeOutMs));

        /*
         * Blocks streaming for given generation until condition is met, or
         * timeout, or interrupted. The condition is met when the checkpoint of
         * previous owning shard has advanced to or beyond the last VLSN of the
         * previous generation. At that time, all writes from previous generation
         * has been streamed from previous owning shard, and it is safe to resume
         * streaming from current shard, after making a checkpoint.
         *
         * For details and more background, please see [#26662].
         */
        final boolean ckptEnabled = otb.getParentRSC().getPu().isCkptEnabled();
        if (!ckptEnabled) {
            /* unit test only */
            logger.info(lm("Unit test only, checkpoint disabled, " +
                           "continue stream partition=" + gen.getPartId() +
                           " (gen=" + gen.getGenNum() + ")" +
                           ", checkpoint at vlsn=" + vlsn));
            return;
        }

        final long ts = System.currentTimeMillis();
        waitForCkpt = true;
        if (PollCondition.await(POLL_INTERVAL_MS, pollTimeOutMs,
                                () -> ckptCatchup(gen))) {
            waitForCkpt = false;
            logger.info(lm(gen.getPartId() + ":" +
                           gen.getPrevGenRepGroup() +
                           " streamed all changes, resume stream from group " +
                           "id=" + otb.getRepGroupId() +
                           ", waiting time(ms)=" +
                           (System.currentTimeMillis() - ts)));
        } else {
            /*
             * Unit test only due to infinite wait
             *
             * Time out in waiting for the green light, have to fail the
             * subscription to signal user that we cannot guarantee that all
             * events from the previous shard are streamed.
             *
             * Before terminating the stream, we make a checkpoint at the
             * open generation vlsn so that user can resume the stream after
             * the gap. The resumed stream will start from the next VLSN
             * after generation is opened.
             */
            final String err = "Timeout in ms=" + pollTimeOutMs +
                               " or interrupted in waiting for open " +
                               "generation=" + gen + " to catch up in " +
                               "previous shard. Terminate stream after " +
                               "checkpoint.";
            waitForCkpt = false;
            logger.warning(lm(err));

            final PublishingUnit pu = otb.getParentRSC().getPu();
            try {
                pu.getCkptTableManager().updateElasticShardCkpt(
                    pu.getStoreName(), pu.getStoreId(), otb.getRepGroupId(),
                    vlsn);
            } catch (CheckpointFailureException cfe) {
                logger.warning("Fail to checkpoint shard=" +
                               otb.getRepGroupId() +
                               " at vlsn=" + vlsn + ", error=" + cfe +
                               (cfe.getCause() == null ? "" :
                                   ", " + cfe.getCause()));
            }

            /*
             * TODO: in the case of store contraction, we may want to ping
             * the shard to sef if it is still live, instead of waiting and
             * timing out if the shard is gone. However it is unclear how to
             * verify if the whole shard, rather than a single node, is still
             * live.
             */

            /* The SFE will surface, close PU and terminate the stream */
            throw new SubscriptionFailureException(sid, err);
        }

        /*
         * Got the green light
         *
         * Make a checkpoint to ensure we won't go back beyond it if failure
         * happens.
         *
         * Now all writes from previous generation has already streamed, and
         * we shall never go back beyond this point if failure happens,
         * otherwise we may end up re-streaming some old generation from
         * this shard only but without re-streaming old generation from
         * other shards, resulting out-of-order events streamed to subscriber.
         *
         * Here is a simple example:
         * Time 1: partition P1 at RG1 with generation 100
         * Time 2: partition P1 moved to RG2 with generation 101
         * Time 3: partition P1 moved back to RG1 with generation 102
         * Time 4 (Now): we know RG2 has streamed all writes of P1 with
         * generation 101, and if failure happens at RG1 later, RG1 should not
         * restart stream from earlier position that may stream writes from
         * generation 100 on this shard.
         */
        try {
            final PublishingUnit pu = otb.getParentRSC().getPu();
            final StreamPosition sp =
                pu.getCkptTableManager()
                  .updateElasticShardCkpt(pu.getStoreName(), pu.getStoreId(),
                                          otb.getRepGroupId(), vlsn);
            logger.info(lm("Done migration checkpoint at position=" + sp));
        } catch (CheckpointFailureException cfe) {
            final String err = "Fail to checkpoint shard=" +
                               otb.getRepGroupId() +
                               " at vlsn=" + vlsn + " for open gen=" + gen;
            logger.warning(lm(err));
            /* we must make the checkpoint, otherwise fail the subscription */
            throw new SubscriptionFailureException(sid, err, cfe);
        }

        /* add a new partition to RSC stat */
        logger.info(lm("Continue stream partition=" + gen.getPartId() +
                       " (gen=" + gen.getGenNum() + ")" +
                       ", checkpoint at vlsn=" + vlsn));
    }

    /**
     * Processes the closed partition generation. It updates the checkpoint
     * table with the given vlsn
      *
     * @param closedGen closed generation
     * @param repGroupId shard id
     * @param ckptVLSN checkpoint vlsn
     * @throws CheckpointFailureException if fail to checkpoint
     */
    void processClosedGen(PartitionGeneration closedGen,
                          RepGroupId repGroupId,
                          long ckptVLSN)
        throws CheckpointFailureException {

        if (closedGen.isOpen()) {
            throw new IllegalArgumentException("Not a closed partition " +
                                               "generation=" + closedGen);
        }

        final PublishingUnit parentPU = otb.getParentRSC().getPu();
        final long ts = System.currentTimeMillis();
        final StreamPosition ckpt =
            parentPU.getCkptTableManager().updateElasticShardCkpt(
                parentPU.getStoreName(), parentPU.getStoreId(),
                repGroupId, ckptVLSN);
        logger.info(lm("Receive close generation=" + closedGen +
                       ", update checkpoint to pos=" + ckpt +
                       ", elapsed time(ms)=" +
                       (System.currentTimeMillis() - ts)));
        if (closedGen.isAllPartClosed()) {
            parentPU.getConsumer(repGroupId).setAllPartClosed();
            logger.info(lm("All generations have closed and streamed"));
        }
    }

    /**
     * Returns true if the processor is blocked and waiting for checkpoint,
     * false otherwise
     */
    boolean isWaitingForCkpt() {
        return waitForCkpt;
    }

    private String lm(String msg) {
        return "[PartGenMarkerProc-" + otb.getParentRSC().getConsumerId() +
               "]" + (useExternalCkpt ? "[External] " : " ") + msg;
    }

    /**
     * Returns true if checkpoint vlsn for given shard must equal or
     * beyond than the given vlsn
     */
    private boolean ckptCatchup(PartitionGeneration openGen) {
        final RepGroupId gid = openGen.getPrevGenRepGroup();
        /* get checkpoint vlsn */
        final long lastCkptVLSN;
        final String ckptTable;
        if (useExternalCkpt) {
            ckptTable = "[external]";
            lastCkptVLSN = getExternalCkptVLSN(gid);
        } else {
            ckptTable = computeCkptTableName(gid);
            lastCkptVLSN = getCkptVLSN(gid, ckptTable);
        }

        if (VLSN.isNull(lastCkptVLSN)) {
            logger.info(lm("Checkpoint for shard=" + gid + " does not " +
                           "exist, checkpoint table=" + ckptTable +
                           ", open generation=" + openGen));
            final NoSQLPublisher pu = otb.getParentRSC().getPu().getParent();
            final Topology topo = pu.getPublisherTopoManager().getTopology();
            final Set<RepGroupId> shards = topo.getRepGroupIds();
            if (!shards.contains(gid)) {
                logger.info(lm("Shard=" + gid + " is gone after store " +
                               "contraction, no need to wait," +
                               "current shards=" + shards));
                return true;
            }
            return false;
        }

        final long vlsn = openGen.getPrevGenEndVLSN();
        if (lastCkptVLSN < vlsn) {
            logger.info(lm("Last checkpoint at shard=" + gid +
                           " is vlsn=" + lastCkptVLSN +
                           ", earlier than vlsn=" + vlsn));
            return false;
        }

        logger.info(lm("Last checkpoint at shard=" +
                       gid + " is vlsn=" + lastCkptVLSN +
                       ", equal or later than vlsn=" + vlsn));
        return true;
    }

    private long getExternalCkptVLSN(RepGroupId gid) {
        if (!useExternalCkpt) {
            throw new IllegalArgumentException("Using internal checkpoint");
        }

        /* use external checkpoint */
        final PublishingUnit pu = otb.getParentRSC().getPu();
        final NoSQLSubscriber subscriber = pu.getSubscriber();
        final StreamPosition ckpt = subscriber.getExternalLastCheckpoint();
        if (ckpt == null) {
            logger.info(lm("No available external checkpoint"));
            return NULL_VLSN;
        }
        final int shardId = gid.getGroupId();
        final StreamPosition.ShardPosition sp = ckpt.getShardPosition(shardId);
        if (sp == null) {
            /* no checkpoint is made for the shard */
            logger.info(lm("Missing checkpoint for shard=" + shardId +
                           ", complete checkpoint=" + ckpt));
            return NULL_VLSN;
        }
        return sp.getVLSN();
    }

    /**
     * Retrieves the last checkpoint vlsn made by internal elastic ops for a
     * shard from a checkpoint table
     * @param gid  shard id
     * @param ckpTable  checkpoint table name
     */
    private long getCkptVLSN(RepGroupId gid, String ckpTable) {

        try {
            /* only get the ckpt for given shard */
            final Set<RepGroupId> shards =
                new HashSet<>(Collections.singletonList(gid));
            final StreamPosition ckpt = otb.getParentRSC().getPu()
                                           .getCkptTableManager()
                                           .fetchElasticCkpt(shards, ckpTable);

            if (ckpt == null) {
                logger.info(lm("No checkpoint is found for shard=" + gid));
                return NULL_VLSN;
            }

            final StreamPosition.ShardPosition shardPos =
                ckpt.getShardPosition(gid.getGroupId());
            if (shardPos == null) {
                /*
                 * ckpt for the given shard does not exist in the checkpoint
                 * record. Possibly because this shard is a new shard
                 */
                logger.info(lm("No position for shard=" + gid +
                               "is found in checkpoint=" + ckpt));
                return NULL_VLSN;
            }

            return shardPos.getVLSN();
        } catch (RequestTimeoutException rte) {
            /* time out in querying server */
            logger.warning(lm("Timeout in querying checkpoint, error=" + rte));
            /*
             * unable to get the checkpoint, simply return null vlsn and
             * the caller will retry after sleep since null vlsn is
             * earlier than any vlsn.
             */
            return NULL_VLSN;
        } catch (Exception exp) {
            logger.warning(lm("Error in querying checkpoint, error=" + exp));
            /*
             * unable to get the checkpoint, simply return null vlsn and
             * the caller will retry after sleep since null vlsn is
             * earlier than any vlsn.
             */
            return NULL_VLSN;
        }
    }

    /**
     * Used in unit test only to shorten test running time
     */
    void setWaitTimeout(int waitTimeoutMs) {
        pollTimeOutMs = waitTimeoutMs;
    }

    /**
     * Computes the checkpoint table name from given shard id. If the shard
     * is covered by this stream, it returns its own checkpoint table name. If
     * the shard is covered by a different stream in the group, it returns the
     * checkpoint table name from the map from configuration.
     *
     * @param gid shard id
     * @return checkpoint table name
     */
    private String computeCkptTableName(RepGroupId gid) {
        final PublishingUnit pu = otb.getParentRSC().getPu();
        final CheckpointTableManager ckptMan = pu.getCkptTableManager();
        if (sid.getTotal() == 1) {
            /* single agent group, return my own checkpoint */
            return ckptMan.getCkptTableName();
        }

        /* the shard is covered by a different stream */
        final int total = sid.getTotal();
        final int idx =
            NoSQLSubscriptionConfig.computeSubscriberIndex(gid, total);
        final String ret = ckptMan.getCheckpointTableName(idx);
        if (ret == null) {
            throw new IllegalStateException(
                "Cannot find checkpoint table for shard id=" + gid +
                ", agent index=" + idx);
        }
        return ret;
    }
}
