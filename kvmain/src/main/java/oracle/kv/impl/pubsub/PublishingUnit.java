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

import static com.sleepycat.je.utilint.VLSN.FIRST_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;
import static oracle.kv.impl.util.ThreadUtils.threadId;

import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.FaultException;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.security.StreamClientAuthHandler;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionChangeNotAppliedException;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.pubsub.SubscriptionInsufficientLogException;
import oracle.kv.pubsub.SubscriptionTableNotFoundException;
import oracle.kv.stats.SubscriptionMetrics;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.stream.FeederFilterChangeResult;
import com.sleepycat.je.utilint.StoppableThread;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Object presents a single publisher unit that services a single subscriber.
 * This PublishingUnit is created when NoSQLPublisher accepts a subscriber and
 * create a subscription for the subscriber. The PublishingUnit is destroyed
 * when a subscription is canceled by subscriber and NoSQLPublisher no longer
 * serves the subscriber.
 */
public class PublishingUnit {

    /**
     * A test hook that can be used to inject simulated insufficient log entry
     * exception and throw {@link SubscriptionInsufficientLogException}. The
     * hook is called in {@link #subscribe(NoSQLSubscriber)} when the the
     * stream establishes connection to the store. The arguments parses the
     * initial stream position to stream from, and the hook would throw
     * {@link SubscriptionInsufficientLogException} to simulate missing log
     * entries.
     */
    public static volatile ExceptionTestHook<StreamPosition,
        SubscriptionInsufficientLogException> ileHook = null;

    /* max wait time to enqueue or dequeue a stream op */
    final static int OUTPUT_QUEUE_TIMEOUT_MS = 1000;

    /* monitor poll internal in seconds */
    private static final int POLL_INTERVAL_SECS = 10;

    /* parent publisher */
    private final NoSQLPublisher parent;
    /* private logger */
    private final Logger logger;
    /* a handle of source kvstore */
    private final KVStoreImpl kvstore;
    /* data consumers for all shards */
    private final ConcurrentMap<RepGroupId, ReplicationStreamConsumer>
        consumers;
    /* shard timeout in ms */
    private final long shardTimeoutMs;
    /* parent publisher directory */
    private final String parentDir;
    /* true if the unit has been closed */
    private final AtomicBoolean closed;
    /* properties for secure store, null for non-secure store */
    private final Properties securityProp;

    /* handle to schedule stat collection thread */
    private volatile ScheduledFuture<?> schedulerHandle;
    /* FIFO queue of stream messages to be consumed by subscriber */
    private volatile BoundedOutputQueue outputQueue;
    /* subscriber using the Publisher */
    private volatile NoSQLSubscriber subscriber;
    /* id of subscriber using the publishing unit */
    private volatile NoSQLSubscriberId si;
    /* shards PU covers, for a single member group, PU covers all shards */
    private volatile Set<RepGroupId> shards;
    /* subscription created by the pu after successful onSubscribe() */
    private volatile NoSQLSubscription subscription;
    /* subscription statistics */
    private volatile SubscriptionStatImpl statistics;
    /* checkpoint table used */
    private volatile CheckpointTableManager ckptTableManager;
    /* true if a new ckpt table is created and used */
    private volatile boolean newCkptTable;

    /*
     * A map to hold impl of all subscribed tables, which will be used in
     * de-serialization of rows. The map is null if user is trying to
     * subscribe all tables in this case, we do not prepare any impl at the
     * time when subscription is created, since new table will be created any
     * time during subscription.
     */
    private volatile ConcurrentMap<String, TableImpl> tables;


    /*-- For test and internal use only. ---*/
    /* true if enable feeder filter */
    private boolean enableFeederFilter;

    /*
     * unit test only, true use table name as id. In unt tests where table
     * created by TableBuilder set id 0 and table API encode table name in
     * the key bytes. The default is false;
     */
    private final boolean tblNameAsId;

    /* subscription config */
    private NoSQLSubscriptionConfig config;

    /* directory of the subscription */
    private String directory;

    /* for scheduled task */
    private final ScheduledExecutorService executor;

    /*
     * the time in ms that PU should expire. The publishing unit should never
     * expire as long as the subscription streams at least one table and
     * hence is not empty. If the subscription is empty, the PU would expire
     * after a given period of time.
     */
    private volatile long expireTimeMs = Long.MAX_VALUE;

    /**
     * Builds a publishing unit for a subscription
     *
     * @param publisher  parent publisher
     * @param logger     logger
     */
    private PublishingUnit(NoSQLPublisher publisher,
                           Logger logger) {
        this(publisher, (KVStoreImpl) publisher.getKvs(),
             publisher.getDefaultShardTimeoutMs(),
             publisher.getJEHASecurityProp(), publisher.getPubRootDir(),
             publisher.useTableNameAsId(), logger);
    }

    /**
     * Unit test only
     */
    public PublishingUnit(NoSQLPublisher parent,
                          KVStoreImpl kvstore,
                          long shardTimeoutMs,
                          Properties securityProp,
                          String parentDir,
                          boolean tblNameAsId,
                          Logger logger) {

        this.parent = parent;
        this.kvstore = kvstore;
        this.shardTimeoutMs = shardTimeoutMs;
        this.securityProp = securityProp;
        this.parentDir = parentDir;
        this.tblNameAsId = tblNameAsId;
        this.logger = logger;

        /* will be init a subscriber establishes a NoSQL Subscription */
        outputQueue = null;
        tables = null;
        consumers = new ConcurrentHashMap<>();
        subscriber = null;
        si = null;
        shards = null;
        closed = new AtomicBoolean(false);
        schedulerHandle = null;
        subscription = null;
        ckptTableManager = null;
        newCkptTable = false;
        config = null;
        directory = null;
        executor = Executors.newSingleThreadScheduledExecutor(
            new PublishingUnitThreadFactory());
    }

    /**
     * Services a request for subscription. It shall not throw any exception
     * to caller.
     *
     * @param pub    parent publisher
     * @param sub    an instance of NoSQLSubscriber
     * @param logger logger
     */
    public static void doSubscribe(NoSQLPublisher pub,
                                   NoSQLSubscriber sub,
                                   Logger logger) {

        final PublishingUnit pu = new PublishingUnit(pub, logger);
        pu.subscribe(sub);
    }

    /**
     * Internally create a subscription
     */
    public void subscribe(NoSQLSubscriber s) {

        /*
         * All exceptions should be taken care of inside the function, and
         * delivered to Subscriber via onError.
         */
        try {
            config = s.getSubscriptionConfig();

            subscriber = s;
            si = config.getSubscriberId();

            /* verify store in init position matches those in publisher */
            checkInitPosition(config);

            /* if a single member group, it contains all shards from topology */
            final Set<RepGroupId> all = kvstore.getTopology().getRepGroupIds();
            shards = NoSQLSubscriptionConfig.computeShards(si, all);
            statistics = new SubscriptionStatImpl(si, shards);
            enableFeederFilter = config.isFeederFilterEnabled();
            ckptTableManager = new CheckpointTableManager(this, logger);
            verifyCkptTable(config);

            /* determine where to start */
            final StreamPosition initPos = getStartStreamPos(config);
            logger.info(lm("Subscription id=" + si + " to stream from " +
                           "shards=" + shards + " out of all=" + all +
                           ", stream mode=" + config.getStreamMode() +
                           ", from position=" + initPos));
            /* prepare the unit */
            prepareUnit(config);

            /* throw simulated SILE */
            assert ExceptionTestHookExecute.doHookIfSet(ileHook, initPos);

            /* shards with insufficient log exception */
            final Map<RepGroupId, Long> ileShards = new HashMap<>();
            /* start all replication stream consumers */
            for (RepGroupId gid : shards) {
                final ReplicationStreamConsumer c = consumers.get(gid);
                final long vlsn = initPos.getShardPosition(gid.getGroupId())
                                         .getVLSN();

                try {
                    c.start(vlsn);
                } catch (InsufficientLogException ile) {
                    /* remember all shards with ILE */
                    ileShards.put(gid, vlsn);
                    continue;
                }

                logger.fine(() -> lm("RSC has started for shard=" + gid +
                                     " from vlsn=" + vlsn + " with mode=" +
                                     config.getStreamMode()));
                statistics.setReqStartVLSN(gid,
                                           c.getRSCStat().getReqStartVLSN());
                statistics.setAckStartVLSN(gid,
                                           c.getRSCStat().getAckedStartVLSN());
            }

            /* throw SILE if insufficient logs  */
            if (!ileShards.isEmpty()) {
                final StringBuilder sb = new StringBuilder();
                for (RepGroupId id : ileShards.keySet()) {
                    sb.append("\n").append("shard=").append(id)
                      .append(", requested start vlsn=")
                      .append(ileShards.get(id));
                }
                /* build SILE with complete error message */
                final SubscriptionInsufficientLogException sile =
                    new SubscriptionInsufficientLogException(
                    si, getSubscribedTables(),
                    "Unable to subscribe due to insufficient logs" +
                    " in at least one shard in source store with" +
                    " stream mode " + config.getStreamMode() +
                    ", insufficient log files in store=" +
                    kvstore.getTopology().getKVStoreName() +
                    " stream mode=" + config.getStreamMode() +
                    ", " + sb);
                ileShards.forEach(sile::addInsufficientLogShard);
                throw sile;
            }

            /* get actual start position */
            final StreamPosition startPos = getActualStartPos();
            /* create a handle to NoSQLSubscription */
            subscription = NoSQLSubscriptionImpl.get(this, s, startPos, logger);
            if (parent == null) {
                /* unit test only */
                logger.fine(() -> lm("Unit test only, no parent publisher."));
            } else {
                /*
                 * Note here we add a PU into the map, officially it becomes an
                 * active subscription, no longer an init subscription, but this
                 * subscription is still counted in the init counter in
                 * publisher, until it is decremented after we return from
                 * subscribe(). Consequently, there is a small window that we
                 * may over count the number of subscriptions in publisher
                 * and make over-conservative (false negative) decision to
                 * reject new subscriptions. That window should be small.
                 */
                parent.addPU(ckptTableManager.getCkptTableName(), this);
                /* ensure the publisher is not closed during initialization */
                if (parent.isClosed()) {
                    throw new SubscriptionFailureException(si,
                                                           "NoSQLPublisher " +
                                                           "has been closed " +
                                                           "during " +
                                                           "initialization, " +
                                                           "need to close and" +
                                                           " exit.",
                                                           null);
                }
            }
            logger.info(lm("Subscription to store=" + getStoreName() +
                           " is created for subscriber=" + si +
                           ", actual start position=" + startPos));

            /*
             * update ckpt table after subscription is successful, the
             * checkpoint may or may not equal to the requested stream
             * position, depending on the stream mode
             */
            if (isCkptEnabled()) {
                doCheckpoint(startPos.prevStreamPosition());
            }

            if (parent == null) {
                /* unit test only */
                logger.info(lm("Monitor thread requires a valid publisher, " +
                               "skip monitor thread in certain unit tests " +
                               "without publisher"));
            } else {
                /* normal case */
                startMonitorTask();
            }

            /*
             * Now we have a full-blown subscription instance to pass to the
             * subscriber.
             *
             * Per Rule Subscription.2 in reactive streams spec, subscription
             * should allow subscriber to call Subscription.request
             * synchronously from within onNext or onSubscribe.
             *
             * https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification
             *
             * User may call Subscription.request in Subscriber.onSubscribe
             * below. Since at this time no subscription worker is available,
             * the subscription will create a separate subscription worker
             * thread to start streaming, and then return immediately.
             */
            try {
                s.onSubscribe(subscription);
            } catch (Exception exp) {
                /* catch all exceptions from user's onSubscribe */
                throw new SubscriptionFailureException(si,
                                                       "Exception raised in " +
                                                       "onSubscribe",
                                                       exp);
            }
        } catch (SubscriptionInsufficientLogException sile) {
            /* normally error msg will be dumped in onError() */
            logger.fine(() -> lm("Insufficient log, error=" + sile));
            /* signal subscriber and close pu */
            close(sile);
        } catch (SubscriptionFailureException sfe) {
            logger.warning(lm("Subscription failed, error=" + sfe));

            /* signal subscriber and close pu */
            close(sfe);
        }

        /* we shall not see any exception other than the above */
    }

    /**
     * Gets checkpoint table name
     *
     * @return checkpoint table name
     */
    String getCkptTableName() {
        return config.getCkptTableName();
    }

    /**
     * Gets checkpoint table name map
     *
     * @return checkpoint table name map
     */
    Map<Integer, String> getCkptTableNameMap() {
        return config.getCkptTableNameMap();
    }

    /**
     * Returns shards covered by this PU, which is available after
     * call of subscriber() is done successfully. If not, returns null.
     *
     * @return  shards covered by this PU
     */
    Set<RepGroupId> getCoveredShards() {
        return shards;
    }

    /**
     * Unit test only.
     *
     * @return true if use table name as id in some unit test
     */
    boolean isUseTblNameAsId() {
        return tblNameAsId;
    }

    /**
     * Returns local address of the publisher
     *
     * @return local address of the publisher
     */
    String getPublisherLocalAddress() {
        if (parent == null) {
            /* unit test */
            return ReplicationStreamConsumer.LOCAL_ADDRESS_IN_TEST;
        }
        return parent.getPublisherLocalAddress();
    }

    /* determine where to start stream */
    private StreamPosition getStartStreamPos(NoSQLSubscriptionConfig conf)
        throws SubscriptionFailureException {

        /* determine where to start */
        StreamPosition initPos;

        if (!isCkptEnabled()) {
            /* checkpoint disabled */
            return (conf.getInitialPosition() != null) ?
                conf.getInitialPosition() :
                StreamPosition.getInitStreamPos(getStoreName(),
                                                getStoreId(),
                                                shards);
        }

        final NoSQLStreamMode mode = conf.getStreamMode();
        switch (mode) {
            case FROM_CHECKPOINT:
            case FROM_EXACT_CHECKPOINT:
                if (newCkptTable) {
                    /* if a new ckpt table, no need to read it */
                    initPos = StreamPosition.getInitStreamPos(getStoreName(),
                                                              getStoreId(),
                                                              shards);
                    final StreamPosition initPosFinal = initPos;
                    logger.fine(() -> lm("a new checkpoint table is created, " +
                                         "start stream from the position=" +
                                         initPosFinal));
                } else {
                    /* if existing ckpt table, fetch checkpoint from it */
                    initPos = getLastCheckpoint();
                    try {
                        initPos = initPos.nextStreamPosition();
                    } catch (ArithmeticException e) {
                        /*
                         * The initial VLSN is set to Long.MAX_VALUE, which
                         * results in the VLSN overflowing when
                         * nextStreamPosition().  It is okay to ignore it in
                         * this case, the VLSN will be set to a real value
                         * later.
                         */
                    }
                    final StreamPosition initPosFinal = initPos;
                    logger.fine(() -> lm("start stream from the next position" +
                                         " after the last checkpoint: " +
                                         initPosFinal));
                }
                break;

            case FROM_STREAM_POSITION:
            case FROM_EXACT_STREAM_POSITION:
                initPos = getInitPosInConf(conf);
                final StreamPosition initPosFinal = initPos;
                logger.fine(() -> lm("start stream from position " +
                                     initPosFinal));
                break;

            case FROM_NOW:
                /* note init pos does not matter */
                initPos = StreamPosition.getInitStreamPos(getStoreName(),
                                                          getStoreId(),
                                                          shards);
                /*
                 * set a meaningless VLSN. Since NULL_VLSN is widely used
                 * through out the code with different semantics, probably
                 * it is better to use Long.MAX_VALUE instead of NULL_VLSN.
                 */
                for (StreamPosition.ShardPosition sp :
                    initPos.getAllShardPos()) {
                    sp.setVlsn(Long.MAX_VALUE);
                }
                logger.fine(() -> lm("start stream with mode " + mode));
                break;

            default:
                throw new SubscriptionFailureException(
                    si, "Invalid stream mode=" + mode);
        }

        return initPos;
    }

    /**
     * Closes a publisher unit, shut down all connection to source and free
     * resources. The close can be called 1) from parent publisher when
     * it is closed by user; 2) from parent publisher when it is unable to
     * create a subscription; 3) from its subscription where it is canceled
     * by user; 4) from underlying child threads when a serious error has
     * happened and the subscription has to be terminated.
     *
     * @param cause  cause to close the publishing unit, null if
     *               normally closed by user without error.
     */
    public void close(Throwable cause) {

        if (!closed.compareAndSet(false, true)) {
            return;
        }

        logger.info(lm("Closing PU starts, cause=" + cause));

        executor.shutdown();

        /* if have a running subscription, cancel it */
        if (subscription != null) {
            ((NoSQLSubscriptionImpl)subscription).cancel(cause);
        }

        /* shut down all rep stream consumers */
        shutDownConsumers();

        /* close bridge queue between consumers and subscriber */
        if (outputQueue != null && !outputQueue.isEmpty()) {
            outputQueue.clear();
        }
        outputQueue = null;

        /* ask parent forget about me */
        if (parent != null) {
            parent.removePU(ckptTableManager.getCkptTableName());
        }

        /* finally signal subscriber */
        assert (subscriber != null);
        if (cause != null) {
            /* close pu due to errors, signal subscriber */
            try {
                subscriber.onError(cause);
            } catch (Exception exp) {
                /* just log, we will close pu anyway */
                logger.warning(lm("Exception in executing " +
                                  "subscriber's onError: " +
                                  exp + "\n" +
                                  LoggerUtils.getStackTrace(exp)));

            }
            logger.info(lm("PU closed due to error=" + cause +
                           (logger.isLoggable(Level.FINE) ?
                               "\n" + LoggerUtils.getStackTrace(cause) : "")));
        } else {
            logger.fine(() -> lm("PU closed normally."));
        }
    }

    /**
     * Gets subscription statistics
     *
     * @return subscription statistics
     */
    public SubscriptionMetrics getStatistics() {
        return statistics;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Gets subscriber using the publisher unit
     *
     * @return subscriber using the publisher unit
     */
    public NoSQLSubscriber getSubscriber() {
        return subscriber;
    }

    /**
     * Retrieves the login handle from kvstore. It should be called only when
     * store is a secure store.
     */
    public LoginHandle getLoginHandle(RepGroupId gid) {
        final LoginManager lm = KVStoreImpl.getLoginManager(kvstore);
        final ShardMasterInfo master = getMasterInfo(gid, null);
        if (master == null) {
            throw new IllegalStateException("Cannot find master for shard=" +
                                            gid);
        }

        return lm.getHandle(master.getMasterRepNodeId());
    }

    /**
     * Returns a handle to kvstore
     *
     * @return a handle to kvstore
     */
    public KVStoreImpl getKVStore() {
        return kvstore;
    }

    /**
     * Returns the security credentials from parent publisher
     *
     * @return the security credentials from parent publisher
     */
    public NoSQLPublisher.SecurityCred getSecurityCred() {
        return parent.getSecurityCred();
    }

    /**
     * Gets the output queue of the unit
     *
     * @return the output queue, null if not initialized
     */
    BoundedOutputQueue getOutputQueue() {
        return outputQueue;
    }

    /**
     * Gets the table md manager from parent publisher
     *
     * @return the table md manager from parent publisher
     */
    PublisherTableMDManager getTableMDManager() {
        return parent.getPublisherTableMDManager();
    }

    /**
     * Gets subscriber index using the publishing unit
     *
     * @return subscriber index using the publishing unit
     */
    NoSQLSubscriberId getSubscriberId() {
        return si;
    }

    /**
     * Internal use in test only
     * <p>
     * We should always get store name and id from the parent
     * publisher. In some unit test the parent publisher might
     * not be available, fetch them from kvstore in that case.
     */
    String getStoreName() {
        if (parent == null) {
            return kvstore.getTopology().getKVStoreName();
        }
        return parent.getStoreName();
    }

    /** Gets the stream position for checkpoint */
    StreamPosition getPositionForCheckpoint(@NonNull StreamPosition candidate) {
        if (subscription == null) {
            /* too early, the subscription not created */
            return null;
        }

        final StreamPosition ret = new StreamPosition(getStoreName(),
                                                      getStoreId());
        consumers.values().forEach(rsc -> {
            final int shardId = rsc.getRepGroupId().getGroupId();
            final FeederFilterStat ffs = rsc.getRSCStat().getFeederFilterStat();
            final long filterVLSN = ffs.getLastFilterVLSN();
            final long passVLSN = ffs.getLastPassVLSN();
            final StreamPosition.ShardPosition sp =
                candidate.getShardPosition(shardId);
            if (sp == null) {
                /* incoming stream position has missing shard */
                return;
            }
            final long currVLSN = sp.getVLSN();
            /*
             * if the current vlsn has been at or surpassed the last vlsn
             * passing the filter, that means the client has received all ops
             * up to the last pass vlsn, and all ops between the filter vlsn
             * and last pass vlsn have been filtered out. Therefore, it is safe
             * to use the filter vlsn as checkpoint, otherwise, just use the
             * vlsn in current stream position.
             */
            final long ckpt = (currVLSN >= passVLSN) ? filterVLSN : currVLSN;
            /*
             * if the filterVLSN is not initialized or feeder stat is stale
             * so that it is earlier than the currVLSN, return the currVLSN.
             *
             */
            final long vlsn = Math.max(ckpt, currVLSN);
            ret.addShardPosition(shardId, vlsn);
        });
        return ret;
    }

    /** Gets the per-shard filter metrics */
    Map<Integer, FeederFilterStat> getFilterMetrics() {
        final Map<Integer, FeederFilterStat> ret = new HashMap<>();
        consumers.values().forEach(
            rsc -> ret.put(rsc.getRepGroupId().getGroupId(),
                           rsc.getRSCStat().getFeederFilterStat()));
        return ret;
    }

    /* stat collection */
    void doStatCollection() {

        /* collect RSC stats */
        for(ReplicationStreamConsumer c : consumers.values()) {
            final RepGroupId id = c.getRepGroupId();
            statistics.updateShardStat(id, c.getRSCStat());
        }

        /* collect other stats */
        final NoSQLSubscriptionImpl sub = (NoSQLSubscriptionImpl)subscription;
        statistics.setTotalConsumedOps(sub.getNumStreamedOps());
    }

    /**
     * Gets master HA hostport from topology manager
     *
     * @param gid                 id of given shard
     * @param currMasterInfo      current master info

     * @return  shard master info, null if cannot find master
     */
    ShardMasterInfo getMasterInfo(RepGroupId gid,
                                  ShardMasterInfo currMasterInfo) {

        if (parent != null) {
            /* normal cases, all query should to go topology manager  */
            try {
                return parent.getPublisherTopoManager()
                             .buildMasterInfo(gid, currMasterInfo);
            } catch (FaultException fe) {
                /* return null and let caller handle the failure */
                logger.warning(lm("Cannot build master for shard=" + gid +
                                  ", reason=" + fe));
                return null;
            }
        }

        /*
         * below are for unit test only, get master feeder HA hostport without
         * a parent publisher
         */

        final PublisherTopoManager topoManager =
            new PublisherTopoManager(getStoreName(), kvstore, logger);
        return topoManager.buildMasterInfo(gid, currMasterInfo);
    }

    /**
     * Gets the last checkpoint made by the subscription
     *
     * @return the last checkpoint
     */
    StreamPosition getLastCheckpoint() {
        return ckptTableManager.fetchCheckpoint(shards);
    }

    /**
     * Internal use in test only
     */
    Topology getTopology() {
        return (kvstore == null) ? null : kvstore.getTopology();
    }

    /**
     * Internal use in test only
     */
    TableAPI getTableAPI() {
        return (kvstore == null) ? null : kvstore.getTableAPI();
    }

    /**
     * Internal use in test only
     */
    public Map<RepGroupId, ReplicationStreamConsumer> getConsumers() {
        return consumers;
    }

    /**
     * Internal use in test only
     */
    public ReplicationStreamConsumer getConsumer(RepGroupId gid) {
        return consumers.get(gid);
    }

    long getShardTimeoutMs() {
        return shardTimeoutMs;
    }

    /* Returns the change timeout */
    long getChangeTimeoutMs() {
        return config.getChangeTimeoutMs();
    }

    /**
     * Returns true if kvstore is a non-secure store
     *
     * @return true if kvstore is a non-secure store, false otherwise.
     */
    boolean isNonSecureStore() {
        /* in unit test without publisher, always non-secure store */
        return parent == null || !parent.isSecureStore();
    }

    CheckpointTableManager getCkptTableManager() {
        return ckptTableManager;
    }

    long getReconnectVLSN(int shardId) {
        final NoSQLStreamMode mode = config.getStreamMode();
        final long ret;
        final StreamPosition currPos = getCurrentPosition();
        if (currPos == null) {
            /*
             * stream not fully created yet, or nothing has been streamed, get
             * resume vlsn from stream config
             */
            final StreamPosition initPos = getStartStreamPos(config);
            ret = initPos.getShardPosition(shardId).getVLSN();
            logger.info(lm("Subscription or current stream " +
                           "position unavailable, reconnect vlsn=" + ret +
                           ", stream mode=" + mode));
            return ret;
        }

        final StreamPosition.ShardPosition sp =
            currPos.getShardPosition(shardId);
        if (sp != null) {
            /*
             * Normal case, stream fully created and data has been streamed
             * from that shard
             */
            ret = sp.getVLSN();
            logger.info(lm("Shard=" + shardId + " reconnect vlsn=" + ret +
                           ", stream mode=" + mode));
        } else {
            /*
             * current stream position available but vlsn not set. e.g,
             * nothing has been streamed from that shard
             */
            final StreamPosition initPos = getStartStreamPos(config);
            ret = initPos.getShardPosition(shardId).getVLSN();
            logger.info(lm("No data streamed from shard=" + shardId + ", " +
                           "reconnect vlsn=" + ret + ", stream mode=" + mode));
        }
        return ret;
    }

    /**
     * Returns current stream position, or null if not available
     */
    private StreamPosition getCurrentPosition() {
        if (subscription == null) {
            return null;
        }
        return subscription.getCurrentPosition();
    }

    /**
     * Returns true if the subscription is not created or has been canceled.
     */
    boolean isSubscriptionCanceled() {
        if (subscription == null) {
            return true;
        }
        return subscription.isCanceled();
    }

    /**
     * Returns true if checkpoint table enabled, false otherwise.
     */
    boolean isCkptEnabled() {
        return config.isCkptEnabled();
    }

    /**
     * Gets the table id string, consider a special unit test case
     */
    static String getTableId(PublishingUnit pu, TableImpl t) {

        if (pu == null  || /* unit test without pu*/
            pu.tblNameAsId /* unit test using table builder */) {
            /*
             * for unit test only.
             *
             * In some unit tests the table created by TableBuilder which sets
             * the id 0, and table API encode table name in the key bytes.
             */
            return t.getFullNamespaceName();
        }
        /* normal cases */
        return t.getIdString();
    }


    /**
     * Applies a change to running subscription.
     *
     * @param changeThread    change request thread
     * @param type            type of change
     * @param tableImpl       table to add
     *
     * @return status of feeder filter change
     */
    synchronized NoSQLSubscriptionImpl.StreamChangeStatus applyChange(
        StoppableThread changeThread, StreamChangeReq.Type type,
        TableImpl tableImpl) throws SubscriptionChangeNotAppliedException {

        logger.fine(() -> lm("Apply change to consumer=" + consumers.keySet() +
                             ", type=" + type + ", " +
                             ", table=" + tableImpl.getFullNamespaceName()));
        final List<ChangeStreamShardResult> result =
            consumers.values().parallelStream()
                     .map(rsc -> rsc.applyChange(changeThread, type, tableImpl))
                     .collect(Collectors.toList());

        /* if subscribed successfully, add it to list */
        return buildChangeResult(result, type,
                                 tableImpl.getFullNamespaceName());
    }

    /**
     * Gets a copy of subscribed tables, or null if subscribing all tables
     *
     * @return a copy of subscribed tables, or null
     */
    Set<String> getSubscribedTables() {
        if (tables == null) {
            return null;
        }

        return new HashSet<>(tables.keySet());
    }

    /**
     * Adds the table to list of subscribed table
     *
     * @param table table to add
     */
    void addTable(TableImpl table) {
        tables.put(table.getFullNamespaceName(), table);
    }

    /**
     * Removes the table from list of subscribed table
     *
     * @param table table to remove
     */
    void removeTable(TableImpl table) {
        tables.remove(table.getFullNamespaceName());
    }

    /**
     * Updates the cached table impl
     *
     * @param type      type of change
     * @param tableImpl table impl
     */
    synchronized void updateCachedTable(StreamChangeReq.Type type,
                                        TableImpl tableImpl) {
        final NoSQLStreamFeederFilter.MatchKey mkey =
            new NoSQLStreamFeederFilter.MatchKey(tableImpl);
        final String rootTid = mkey.getRootTableId();
        final String tid = mkey.getTableId();
        if (type.equals(StreamChangeReq.Type.ADD)) {
            consumers.values()
                     .forEach(rsc -> rsc.addCachedTable(rootTid, tableImpl));
        } else if (type.equals(StreamChangeReq.Type.REMOVE)) {
            consumers.values()
                     .forEach(rsc -> rsc.removeCachedTable(rootTid, tid));
        } else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }

        logger.fine(() -> lm("Cached table impl updated with change, type=" +
                             type + ", table=" + tableImpl.getFullName()));
    }

    /**
     * Returns true if the table is subscribed, false otherwise. If the
     * stream is configured to subscribe all tables, return true if the table
     * exists, false otherwise.
     *
     * @param tableName name of table
     *
     * @return  true if the table is subscribed, false otherwise
     */
    boolean isTableSubscribed(String tableName) {

        /* wide-card stream */
        final Set<String> initTables =
            subscriber.getSubscriptionConfig().getTables();
        if (initTables == null || tables == null) {
            return parent.getPublisherTableMDManager()
                         .getTable(si, tableName) != null;
        }

        /* empty stream */
        if (tables.isEmpty()) {
            return false;
        }

        /* non-empty stream */
        return tables.containsKey(tableName);
    }

    /* sets expiration time if empty steam */
    synchronized void setExpireTimeMs() {
        if (!getSubscribedTables().isEmpty()) {
            return;
        }
        expireTimeMs = System.currentTimeMillis() +
                       config.getEmptyStreamSecs() * 1000L;
        logger.fine(() -> lm("Empty stream will expire in time(secs)=" +
                             config.getEmptyStreamSecs()));
    }

    /* clears expiration time */
    synchronized void unsetExpireTimeMs() {
        if (expireTimeMs < Long.MAX_VALUE) {
            expireTimeMs = Long.MAX_VALUE;
            logger.fine(() -> lm("Stream is no longer empty and will never " +
                                 "expire."));
        }
    }


    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[PU-" +
               (ckptTableManager == null ? "<na>" :
                   ckptTableManager.getCkptTableName()) +
               "-" + si + "] " + msg;
    }

    /* verify store in config matches those in publisher */
    private void checkInitPosition(NoSQLSubscriptionConfig conf)
        throws SubscriptionFailureException {

        /* bypass check if init stream position not specified */
        if (conf.getInitialPosition() == null) {
            return;
        }

        final String storeInConfig = conf.getInitialPosition().getStoreName();
        final long storeIdInConfig = conf.getInitialPosition().getStoreId();

        if (!storeInConfig.equals(getStoreName())) {
            throw new SubscriptionFailureException(si,
                "Store  " + storeInConfig + " in init stream position " +
                "does not match the store in publisher: " + getStoreName());
        }

        final long storeId = kvstore.getTopology().getId();
        if (storeIdInConfig != storeId) {
            throw new SubscriptionFailureException(si,
                "Store id " + storeIdInConfig + " in stream position " +
                "does not match the store id in publisher: " + storeId);
        }
    }

    /* clean up all consumers */
    private void shutDownConsumers() {
        /* shutdown all consumers */
        for (ReplicationStreamConsumer client : consumers.values()) {
            client.cancel(true);
        }
        consumers.clear();
        logger.fine(() -> lm("All RSC shut down."));

        /* cancel stat collection */
        if (schedulerHandle != null && !schedulerHandle.isCancelled()) {
            schedulerHandle.cancel(false);
            schedulerHandle = null;
        }
    }

    /* prepare publisher and make it ready to stream */
    private void prepareUnit(NoSQLSubscriptionConfig conf)
        throws SubscriptionFailureException {

        final int sz = conf.getTotalOutputQueueSizeMB();
        outputQueue = new BoundedOutputQueue(si, getCkptTableName(),sz, logger);
        logger.info(lm("PU starts with covered shards=" + shards +
                       ", output queue in MB=" + sz));
        /*
         * ensure subscribed tables all valid, happens at the beginning
         * of subscription when a publishing unit is created.
         *
         * When a table is removed in during an ongoing subscription, it
         * is transparent to subscriber and subscriber will not receive
         * any updates from that table. If a table is added in the mid
         * of subscription, it is transparent to subscriber too, since
         * the new table is not subscribed. The only exception is that
         * when user subscribes all tables without specifying any table
         * name, subscriber will receive updates to the new table when
         * it is created and populated.
         */
        final Set<String> tableNames = conf.getTables();
        if (tableNames != null) {/* not a wide-card stream */
            /* user subscribes some tables */
            tables = new ConcurrentHashMap<>();
            Set<String> notFoundTbls = null;
            for (String table : tableNames) {

                /* ask table md manager for md, create a local copy */
                TableImpl tableImpl = getTable(conf.getSubscriberId(), table);

                if (tableImpl == null) {
                    final String err = "Table=" + table + " not found at " +
                                       "store";
                    logger.warning(lm(err));
                    if (notFoundTbls == null) {
                        notFoundTbls = new HashSet<>();
                    }
                    notFoundTbls.add(table);
                } else {
                    tables.put(tableImpl.getFullNamespaceName(), tableImpl);
                }
            }

            if (notFoundTbls != null) {
                /* some tables not found, throw SFE */
                final String err = "Tables not found=" + notFoundTbls;
                final SubscriptionTableNotFoundException stnfe =
                    new SubscriptionTableNotFoundException(
                        notFoundTbls.toArray(new String[0]));
                throw new SubscriptionFailureException(si, err, stnfe);
            }

            logger.fine(() -> lm("PU subscribed tables=" + tables.keySet() +
                                 ", table names=" + tableNames));
        } else {
            /* user subscribes all tables */
            logger.fine(() -> lm("PU subscribed all tables"));
        }

        /*
         * ensure directory exists for the subscription. User may re-create a
         * subscription on the same publisher after cancel an existing one, so
         * allow pre-exist directory. Duplicate subscription wont take place
         * because feeder will reject all duplicate connections.
         */
        try {
            directory = NoSQLPublisher.ensureDir(parentDir,
                                                 conf.getCkptTableName(),
                                                 true);
            logger.fine(() -> lm("Directory ok at path=" + directory));
        } catch (Exception cause) {
            /* capture all exceptions and raise pfe to user */
            final String err = "Fail to ensure directory (parent dir=" +
                               parentDir + ", child=" +
                               conf.getCkptTableName() + ")";
            throw new SubscriptionFailureException(si, err, cause);
        }

        for (RepGroupId gid : shards) {
            final ShardMasterInfo master = getMasterInfo(gid, null);
            if (master == null) {
                final String err = "Unable to get master info for shard=" + gid;
                throw new SubscriptionFailureException(si, err);
            }

            try {
                final ReplicationStreamConsumer consumer =
                    new ReplicationStreamConsumer(
                        this,
                        master,
                        gid,
                        outputQueue,
                        ((tables == null) ? null : tables.values()),
                        directory,
                        getFilter(gid),
                        conf.getStreamMode(),
                        conf.getMaxReconnect(),
                        conf.getStreamOperationDeserializer(),
                        securityProp,
                        logger);
                if (conf.getILETestHook() != null) {
                    /* unit test only */
                    consumer.setILEHook(conf.getILETestHook());
                }
                if (conf.getTestToken() != null) {
                    /* unit test only */
                    final StreamClientAuthHandler authHandler =
                        (StreamClientAuthHandler)consumer.getAuthHandler();
                    authHandler.setToken(conf.getTestToken());
                }

                consumers.put(gid, consumer);
                logger.fine(() -> lm("RSC ready for shard=" + gid +
                                     ", master=" + master));
            } catch (UnknownHostException uhe) {
                final String err = "Unknown host for shard=" + gid +
                                   ", master=" + master;
                logger.warning(lm(err));
                throw new SubscriptionFailureException(si, err, uhe);
            }
        }

        logger.fine(() -> lm("PU preparation done."));
    }

    /* Creates feeder filter from subscribed tables */
    private NoSQLStreamFeederFilter getFilter(RepGroupId gid) {

        /* no feeder filter if disabled */
        if (!enableFeederFilter) {
            return null;
        }

        final int nParts = getTotalNumParts();
        final NoSQLStreamFeederFilter filter;
        final boolean localWrites = config.isLocalWritesOnly();
        final int localRegionId = config.getLocalRegionId();
        if (tables == null) {
            /* get a feeder filter passing all tables */
            filter = NoSQLStreamFeederFilter.getFilter(
                null, nParts, localWrites, localRegionId);
        } else if (tables.isEmpty()) {
            /* get a feeder filter passing no table */
            filter = NoSQLStreamFeederFilter.getFilter(
                new HashSet<>(), nParts, localWrites, localRegionId);
        } else {
            /* get a feeder filter passing selected tables */
            filter = NoSQLStreamFeederFilter.getFilter(
                new HashSet<>(tables.values()), nParts, localWrites,
                localRegionId);
        }
        filter.setRepGroupId(gid);
        logger.fine(() -> lm("Filter created with tables=" +
                             ((tables == null || tables.isEmpty()) ?
                                 "all user tables" : tables.keySet()) +
                             ", filter=" + filter));
        return filter;
    }

    /* gets a table impl from table md */
    TableImpl getTable(NoSQLSubscriberId sid, String table) {

        /* unit test only, no parent publisher */
        if (parent == null) {
            return (TableImpl) kvstore.getTableAPI().getTable(table).clone();
        }

        /* normal cases, all query should to go tableMD manager */
        return parent.getPublisherTableMDManager().getTable(sid, table);
    }

    /*
     * We should always get store name and id from the parent
     * publisher. In some unit test the parent publisher might
     * not be available, fetch them from kvstore in that case.
     */
    long getStoreId() {
        if (parent == null) {
            return kvstore.getTopology().getId();
        }
        return parent.getStoreId();
    }

    NoSQLPublisher getParent() {
        return parent;
    }

    private void verifyCkptTable(NoSQLSubscriptionConfig conf) {

        if (!isCkptEnabled()) {
            /* unit test only */
            logger.warning(lm("Checkpoint disabled"));
            return;
        }

        try {
            if (ckptTableManager.isCkptTableExists()) {
                logger.info(lm("Checkpoint table=" + conf.getCkptTableName() +
                               " already exists"));
                return;
            }

            if (conf.useNewCheckpointTable()) {
                /* ok, we can create a new ckpt table */
                final boolean multiSubs = (si.getTotal() > 1);
                ckptTableManager.createCkptTable(multiSubs);
                newCkptTable = true;
                logger.info(lm("Checkpoint table=" + conf.getCkptTableName() +
                               " is created"));
                return;
            }
        } catch (FaultException fe) {
            final String err = "Subscription=" + conf.getSubscriberId() +
                               " gets fault exception when attempt to " +
                               "ensure checkpoint table=" +
                               ckptTableManager.getCkptTableName() +
                               " at store=" + getStoreName();
            logger.warning(lm(err + ", cause=" + fe));
            throw new SubscriptionFailureException(conf.getSubscriberId(),
                                                   err + ", cause=" +
                                                   exceptionString(fe),
                                                   fe);
        } catch (UnauthorizedException ue) {
            final String err = "Subscription " + conf.getSubscriberId() +
                               " has insufficient privilege to create table " +
                               ckptTableManager.getCkptTableName() +
                               " at store " + getStoreName();
            logger.warning(lm(err + ", cause=" + ue));
            throw new SubscriptionFailureException(conf.getSubscriberId(),
                                                   err + ", cause=" +
                                                   exceptionString(ue),
                                                   ue);
        }

        /* ckpt table does not exist and we are not allowed to create it */
        final String err = "Subscription=" + conf.getSubscriberId() +
                           " configured to use existing checkpoint " +
                           "table=" +
                           ckptTableManager.getCkptTableName() +
                           " while table not found at store=" +
                           getStoreName();
        logger.warning(lm(err));
        throw new SubscriptionFailureException(conf.getSubscriberId(),
                                               err);
    }

    private void doCheckpoint(StreamPosition pos)
        throws SubscriptionFailureException {

        try {
            ckptTableManager.updateCkptTableInTxn(pos);
        } catch (CheckpointFailureException cfe) {
            final String err = "Unable to update checkpoint table=" +
                               cfe.getCheckpointTableName() +
                               " with the initial stream position=" +
                               pos;
            logger.warning(lm(err));
            throw new SubscriptionFailureException(si, err, cfe);
        }
    }

    /* get init stream position from config */
    private StreamPosition getInitPosInConf(NoSQLSubscriptionConfig conf) {

        /* use position specified in config */
        StreamPosition initPos = conf.getInitialPosition();
        if (initPos == null) {
            initPos = StreamPosition.getInitStreamPos(getStoreName(),
                                                      getStoreId(),
                                                      shards);
            final String pos = initPos.toString();
            logger.fine(() -> lm("initial position not specified, " +
                                 "will start stream from position=" + pos));
        } else {
            /* sanity check if user specifies an init position */
            for (RepGroupId gid : shards) {
                /* missing shard in init position */
                if (initPos.getShardPosition(gid.getGroupId()) == null) {
                    throw new SubscriptionFailureException(
                        si, "Incomplete user specified init stream " +
                            "position, missing position for " +
                            "shard=" + gid);
                }
            }

            /*
             * user should specify a valid start vlsn for each shard covered
             * by this PU, if not, it is an user error and user need to fix
             * it to subscribe.
             */
            for (StreamPosition.ShardPosition p : initPos.getAllShardPos()) {
                final int gid = p.getRepGroupId();
                if (!shards.contains(new RepGroupId(gid))) {
                    /*
                     * user specified a position of shard PU does not cover,
                     * dump warning and ignore.
                     */
                    logger.warning(lm("Uncovered shard=" + gid +
                                      " in init stream position, shards " +
                                      "covered=" + shards));
                    continue;
                }

                if (p.getVLSN() == NULL_VLSN) {
                    throw new SubscriptionFailureException(
                        si, "User specified start VLSN for shard=" +
                            p.getRepGroupId() + " is null");
                }
            }
            final String pos = initPos.toString();
            logger.fine(() -> lm("PU to start stream from a given init " +
                                 "position=" + pos));
        }

        return initPos;
    }

    /* Collects actual start position for each shard */
    private StreamPosition getActualStartPos() {
        final StreamPosition ret = new StreamPosition(getStoreName(),
                                                      getStoreId());
        final Map<RepGroupId, Long> actualStart = statistics.getAckStartVLSN();
        for (RepGroupId rid : shards) {
            ret.setShardPosition(rid.getGroupId(),  actualStart.get(rid));
        }
        return ret;
    }

    /* Gets the total number of partitions in store */
    private int getTotalNumParts() {
        return kvstore.getTopology().getPartitionMap().getNPartitions();
    }

    private void handleStoreContraction() {
        final Set<RepGroupId> toRemove = new HashSet<>();
        /* find all shards that all partitions have moved out */
        consumers.values().stream()
                 .filter(this::isRSCClosable)
                 .forEach(
                     rsc -> {
                         logger.info(lm("Shard=" + rsc.getRepGroupId() +
                                        " retired and all closed " +
                                        "partitions streamed, shut down" +
                                        " RSC"));
                         rsc.cancel(true);
                         toRemove.add(rsc.getRepGroupId());
                     }
                 );
        toRemove.forEach(gid -> {
            shards.remove(gid);
            consumers.remove(gid);
        });

        if (toRemove.isEmpty()) {
            logger.fine(() -> lm("No need to shutdown any RSC"));
        } else {
            logger.info(lm("PU shuts down RSC for shards=" + toRemove));
        }

        /*
         * Wait for the topology to be refreshed, such that it does not have the
         * retired shards, in order to prevent these retired shards looking
         * like a store expansion and being added back as a new shards
         */
        if (!toRemove.isEmpty() &&
            PollCondition.await(1000, Long.MAX_VALUE,
                                () -> shardsRemoved(toRemove))) {
            logger.fine(() -> lm("Shards=" + toRemove + " have been " +
                                 "removed from topology"));
        }
    }

    /* returns true if the given shards have been removed from topology */
    private boolean shardsRemoved(Set<RepGroupId> toRemove) {
        final Set<RepGroupId> ids =
            parent.getPublisherTopoManager().getTopology().getRepGroupIds();
        final Set<RepGroupId> retired = toRemove.stream().filter(ids::contains)
                                                .collect(Collectors.toSet());
        if(retired.isEmpty()) {
            return true;
        }
        logger.fine(() -> lm("Retired shards still exist in topology=" +
                             retired));
        return false;
    }

    /* returns true if the RSC should be closed */
    private boolean isRSCClosable(ReplicationStreamConsumer rsc) {
        final Topology topo = parent.getPublisherTopoManager().getTopology();
        final RepGroupId gid = rsc.getRepGroupId();
        if (!topo.getRepGroupIds().contains(gid)) {
            /*
             * topology has been updated to remove the retired shard, check
             * if all closed partitions have been streamed
             */
            if (rsc.isAllPartClosed()) {
                logger.info(lm("Topology no longer has rep group=" + gid +
                               " and all closed partitions have " +
                               "been streamed."));
                return true;
            }

            /*
             * Topology has been updated to remove the shard, but we have
             * not received all entries from all closed partitions on that
             * shard, return not-closable to keep RSC live and the caller
             * should check later.
             */
            logger.fine(() -> lm("Topology no longer has rep group=" + gid +
                                 " but not all closed partitions have been" +
                                 " streamed."));
            return false;
        }

        /* check if the shard owns any partition */
        final PartitionMap pm = topo.getPartitionMap();
        final Set<PartitionId> owned =
            pm.getAllIds().stream().filter(
                pid -> pm.getRepGroupId(pid).equals(gid))
              .collect(Collectors.toSet());

        if (owned.isEmpty()) {
            /*
             * The shard does not own any partition
             *
             * case 1: it is a new born shard, not closeable
             * case 2: it is a regular shard, all partitions migrated out,
             * closeable if all closed partition streamed, not closeable
             * otherwise.
             */
            if (rsc.isNewBornShard()) {
                logger.info(lm("New born rep group=" + gid + " owns no " +
                               "partition, not closeable"));
                return false;
            }


            if (rsc.isAllPartClosed()) {
                logger.info(lm("Rep group=" + gid + " no longer owns any" +
                               " partition and all closed generations " +
                               "have been streamed, RSC is closeable"));
                return true;
            }

            logger.info(lm("Rep group=" + gid + " no longer owns any " +
                           "partition but not all closed generation " +
                           "have been streamed, RSC is not closeable"));
            return false;
        }

        /*
         * the shard owns at least one partition, clear the flag if new born
         * shard. Obviously not closeable
         */
        if (rsc.isNewBornShard()) {
            rsc.clearNewBornShard();
            logger.info(lm("Rep group=" + gid + " owns partition=" +
                           owned + ", clear new born flag"));
        }

        return false;
    }

    private void handleStoreExpansion() {

        /* find new shard from topology */
        final List<RepGroupId> moreShards =
            parent.getPublisherTopoManager().getTopology()
                  .getRepGroupIds().stream()
                  /* new shard only */
                  .filter(gid -> !consumers.containsKey(gid))
                  /* only cares my shard */
                  .filter(gid -> NoSQLSubscriptionConfig
                      .subscriberIncludesShard(si, gid))
                  .collect(Collectors.toList());

        if (moreShards.isEmpty()) {
            logger.fine(() -> lm("No new shard detected."));
            return;
        }

        logger.info(lm("Detect new shards=" + moreShards));

        shards.addAll(moreShards);
        /*
         * new shard always start from vlsn=1, filter would build partition
         * list and only allow writes to the new partition to pass.
         */
        final NoSQLStreamMode mode = NoSQLStreamMode.FROM_STREAM_POSITION;
        /* start rsc for each new shard */
        for(RepGroupId gid : moreShards) {
            final ShardMasterInfo master = getMasterInfo(gid, null);
            try {
                final ReplicationStreamConsumer c =
                    new ReplicationStreamConsumer(
                        this,
                        master,
                        gid,
                        outputQueue,
                        ((tables == null) ?
                            null : tables.values()),
                        directory,
                        getFilter(gid),
                        mode,
                        config.getMaxReconnect(),
                        config.getStreamOperationDeserializer(),
                        securityProp,
                        logger);

                /* a new board shard during expansion */
                c.setNewBornShard();

                consumers.put(gid, c);
                logger.info(lm("RSC created for new shard=" + gid +
                               " master=" + master));

                c.start(FIRST_VLSN);
                final long reqStartVLSN = c.getRSCStat().getReqStartVLSN();
                final long ackStartVLSN = c.getRSCStat().getAckedStartVLSN();
                statistics.setReqStartVLSN(gid, reqStartVLSN);
                statistics.setAckStartVLSN(gid, ackStartVLSN);
                if (ackStartVLSN == reqStartVLSN) {
                    logger.info(lm("new RSC has started for shard=" + gid +
                                   " from vlsn=" + reqStartVLSN +
                                   " with mode=" + mode));
                } else {
                    logger.info(lm("new RSC has started for shard=" + gid +
                                   ", req. start vlsn=" + reqStartVLSN +
                                   ", higher ack. vlsn=" + ackStartVLSN +
                                   ", mode=" + mode));
                }
            } catch (InsufficientLogException ile) {
                /* should be very unlikely that a new shard got ILE */
                final String err =
                    "Unable to subscribe due to insufficient logs" +
                    " at new shard=" + gid +
                    " stream mode=" + config.getStreamMode() +
                    ", error=" + ile;
                logger.warning(err);
                final SubscriptionInsufficientLogException sife =
                    new SubscriptionInsufficientLogException(
                        si, getSubscribedTables(), err);
                sife.addInsufficientLogShard(gid, FIRST_VLSN);
                throw sife;
            } catch (Exception exp) {
                final String err = "Cannot create stream to new shard=" + gid;
                logger.warning(err + ", error=" + exp);
                throw new SubscriptionFailureException(
                    si, err + ", error=" + exceptionString(exp), exp);
            }
        }
    }

    /*
     * Why do we need a monitor task to poll the topology periodically here?
     *
     * Polling is used to detect cases where elasticity changes have modified
     * the set of feeders this publishing unit needs to stream changes from.
     * Changes to the set of feeders occur in two cases.
     *
     * In a store expansion that adds new shards, a new shard may be owned by a
     * feeder that this publishing unit is not currently in contact with.  In
     * that case, the publishing unit needs to detect that it should establish
     * a connection to the new feeder.  The feeder in question is not in
     * contact with this particular publishing unit, and does not have an easy
     * way to determine which, or contact, publishing units might be interested
     * in receiving its records, so it can't signal the publishing unit of the
     * change.  Other feeders that are in contact with the publishing unit are
     * not likely to be aware that the change occurred, so it is up to the
     * publishing unit to detect the change itself.  It does that by polling
     * the topology maintained by its request dispatcher, computing the set of
     * feeders that it needs to stream operations from, and establishing new
     * connections as needed.  Because the stream API delivers operations from
     * each shard independently, there is no requirement beyond quality of
     * service to start the stream of changes from a new shard, and so polling
     * is sufficiently timely.
     *
     * During a store contraction, a shard removed from the store may be the
     * last shard owned by a particular feeder that is connected to this
     * publishing unit.  In that case, the publishing unit should terminate its
     * connection to that feeder since the connection will no longer stream any
     * entries for this publishing unit.  The publishing unit is connected to
     * the feeder at the source end of the partition migration, which does not
     * have definitive information about when the removal of the partition is
     * complete.  As a result, here, too, the publishing unit checks the
     * topology to determine whether a feeder connection is no longer needed.
     * Since the feeder whose connection will be terminated is no longer
     * sending updates, termination merely reduces resource use and can
     * therefore be performed whenever it is convenient.
     */
    private void startMonitorTask() {
        executor.scheduleAtFixedRate(
            () -> {
                try {
                    handleStoreContraction();
                    handleStoreExpansion();
                    handleEmptyStream();
                } catch (Exception cause) {
                    logger.warning(lm("Shutdown subscription=" + si + " " +
                                      "because monitor task exits due to " +
                                      "error=" + cause));
                    close(cause);
                } finally {
                    logger.fine(() -> lm("Monitor thread exits"));
                }
            },
            POLL_INTERVAL_SECS, POLL_INTERVAL_SECS, TimeUnit.SECONDS);

        logger.info(lm("Monitor task started to keep track of store " +
                       "expansion and contraction, scheduled interval " +
                       "in time(secs)=" + POLL_INTERVAL_SECS));
    }

    /**
     * Generates the change result status from shard result
     *
     * @param res        shard change subscription result
     *
     * @return subscription change result status
     */
    private NoSQLSubscriptionImpl.StreamChangeStatus buildChangeResult(
        List<ChangeStreamShardResult> res, StreamChangeReq.Type type,
        String tb) {

        /* log all per-shard results */
        res.sort(Comparator.comparingInt(ChangeStreamShardResult::getShardId));
        /* build message of result from shards */
        final StringBuilder sb = new StringBuilder();
        res.forEach(r -> sb.append(r.toString()).append("\n"));

        /* successful if and only if every shard is successful */
        final boolean successAllShards =
            res.stream()
               .allMatch(r -> r.getResult() != null &&
                            r.getResult().getStatus().equals(
                                FeederFilterChangeResult.Status.OK));
        /* every shard is good */
        if (successAllShards) {
            logger.info(lm("All shards have completed change stream request" +
                           ", type=" + type + ", table=" + tb +
                           ", result=\n" + sb));
            return NoSQLSubscriptionImpl.StreamChangeStatus.OK;
        }

        /* Return null if the change is not applicable for every shard */
        final boolean allNotApplicable =
            res.stream()
               .allMatch(r -> r.getResult() != null &&
                              r.getResult().getStatus().equals(
                                FeederFilterChangeResult.Status
                                    .NOT_APPLICABLE));
        if (allNotApplicable) {
            /* change not applicable to all shards */
            logger.info(lm("Change not applicable in all shards" +
                           ", type=" + type + ", table=" + tb +
                           ", result=\n" + sb));
            return NoSQLSubscriptionImpl.StreamChangeStatus.NOT_APPLICABLE;
        }

        /* timeout if any shard times out */
        final Set<ChangeStreamShardResult> to =
            res.stream()
               .filter(r -> (r.getLocalCause() instanceof TimeoutException))
               .collect(Collectors.toSet());
        if (!to.isEmpty()) {
            logger.info(lm("Timeout in changing result for shard=" +
                           to.stream().map(ChangeStreamShardResult::getShardId)
                             .collect(Collectors.toSet()) +
                           ", type=" + type + ", table=" + tb +
                           ", result=" + sb));
            return NoSQLSubscriptionImpl.StreamChangeStatus.TIMEOUT;
        }

        logger.warning(lm("Fail to change stream, type=" + type +
                          ", table=" + tb + ", result=\n" + sb));
        return NoSQLSubscriptionImpl.StreamChangeStatus.FAIL;
    }

    /**
     * Shuts down the empty stream if timeout
     */
    private void handleEmptyStream() {

        /* return if wildcard stream */
        if (config.isWildCardStream() || tables == null) {
            return;
        }

        /* return if stream is not empty */
        if (!tables.isEmpty()) {
            return;
        }

        if (System.currentTimeMillis() > expireTimeMs) {
            final String err = "Empty stream expires, will shut down";
            /* event is not common and important enough to log */
            logger.log(Level.INFO, () -> lm(err));
            throw new EmptyStreamExpireException(config.getSubscriberId(), err);
        }
    }

    /* Exception thrown when the stream expires */
    public static class EmptyStreamExpireException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final NoSQLSubscriberId sid;
        EmptyStreamExpireException(NoSQLSubscriberId sid, String error) {
            super(error);
            this.sid = sid;
        }
        public NoSQLSubscriberId getSid() {
            return sid;
        }
    }

    /**
     * A KV thread factory that logs if a thread exits on an unexpected
     * exception.
     */
    private class PublishingUnitThreadFactory extends KVThreadFactory {
        PublishingUnitThreadFactory() {
            super("PublishingUnitThreadFactory", logger);
        }

        @Override
        public Thread.UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return (thread, ex) ->
                logger.warning(lm("PU thread=" + thread.getName() +
                                  "(id=" + threadId(thread) + ")" +
                                  " exits unexpectedly, error=" + ex +
                                  "\nstack=" + LoggerUtils.getStackTrace(ex)));
        }
    }

    /**
     * Returns minimal stream position from open transaction buffer and its
     * output queue. The returned position is built from all replication
     * stream consumers.
     */
    StreamPosition getMinPosFromOTB(String store, long storeId) {
        final StreamPosition ret = new StreamPosition(store, storeId);
        /* get the min vlsn for each shard */
        consumers.keySet().forEach(k -> {
            final int shardId = k.getGroupId();
            final ReplicationStreamConsumer rsc = consumers.get(k);
            ret.addShardPosition(shardId, rsc.getTxnBuffer().getMinVLSN());
        });
        return ret;
    }

    public static class BoundedOutputQueue {

        /** wait time in ms in enqueue and dequeue */
        private static final long QUEUE_WAIT_MS = 100;
        /** rate limiting logging interval in ms */
        private static final int RL_LOG_PERIOD_MS = 10 * 60 * 1000;
        /** rate limiting logger max # of objects */
        private static final int RL_MAX_OBJ = 1024;
        /** object identifier in rate limiting logging in enqueue */
        private static final String ENQUEUE_RL_LOG = "EnqueueRL";
        /** object identifier in rate limiting logging in dequeue */
        private static final String DEQUEUE_RL_LOG = "DequeueRL";

        /** subscriber id */
        private final NoSQLSubscriberId sid;
        /** name of checkpoint */
        private final String ckptTableName;
        /** rate limiting logger */
        private final RateLimitingLogger<String> rlLogger;
        /** FIFO queue of stream messages to be consumed by subscriber */
        private final BlockingQueue<StreamOperation> boundedQueue;
        /** size limit of the queue */
        private final long maxSizeBytes;
        /** current size of queue */
        private volatile long currSizeBytes;
        /** private logger */
        private final Logger logger;

        public BoundedOutputQueue(NoSQLSubscriberId sid,
                                  String ckptTableName,
                                  int maxSizeMB,
                                  Logger logger) {
            this.sid = sid;
            this.ckptTableName = ckptTableName;
            this.logger = logger;
            maxSizeBytes = maxSizeMB * 1024L * 1024L;
            boundedQueue = new LinkedBlockingDeque<>();
            currSizeBytes = 0;
            rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS,
                                                RL_MAX_OBJ, logger);
        }

        public void clear() {
            synchronized (boundedQueue) {
                boundedQueue.clear();
                currSizeBytes = 0;
            }
        }
        public boolean isEmpty() {
            return boundedQueue.isEmpty();
        }

        /**
         * Enqueues a stream operation from the bounded queue, wait till
         * space is available if queue is full, or return if in shutdown.
         *
         * @param shutdown supplier to check if in shutdown
         * @throws InterruptedException if interrupted.
         */
        public void enqueue(StreamOperation op, BooleanSupplier shutdown)
            throws InterruptedException {
            final long sz = getSizeBytes(op);
            final long start = System.currentTimeMillis();
            synchronized (boundedQueue) {
                while (overflow(sz)) {
                    boundedQueue.wait(QUEUE_WAIT_MS);
                    if (shutdown.getAsBoolean()) {
                        /* stop wait if shutdown */
                        logger.info(lm("Exit enqueue since subscription is " +
                                       "canceled."));
                        return;
                    }
                    rlLogger.log(ENQUEUE_RL_LOG, Level.INFO,
                                 lm("Queue overflow (sizeMB=" +
                                    (maxSizeBytes / (1024 * 1024)) + ")" +
                                    ", #ops in queue=" + boundedQueue.size() +
                                    ", op size in bytes=" + sz +
                                    ", elapsed time ms=" +
                                    (System.currentTimeMillis() - start)));
                }
                /* enough space, enqueue and increment size stat */
                boundedQueue.put(op);
                currSizeBytes += sz;
                boundedQueue.notifyAll();
            }
        }

        /**
         * Dequeues a stream operation from the bounded queue, wait till an
         * entry is available, or return null if in shutdown.
         *
         * @param shutdown supplier to check if in shutdown
         * @return a stream operation or null
         * @throws InterruptedException if interrupted.
         */
        public StreamOperation dequeue(BooleanSupplier shutdown)
            throws InterruptedException {
            final StreamOperation ret;
            final long start = System.currentTimeMillis();
            synchronized (boundedQueue) {
                while (boundedQueue.isEmpty()) {
                    boundedQueue.wait(QUEUE_WAIT_MS);
                    if (shutdown.getAsBoolean()) {
                        /* stop wait if shutdown */
                        logger.info(lm("Exit dequeue since subscription is " +
                                       "canceled."));
                        return null;
                    }
                    rlLogger.log(DEQUEUE_RL_LOG, Level.INFO,
                                 lm("Empty queue, elapsed time ms=" +
                                    (System.currentTimeMillis() - start)));
                }
                /* dequeue an operation */
                ret = boundedQueue.take();
                currSizeBytes -= getSizeBytes(ret);
                boundedQueue.notifyAll();
            }
            return ret;
        }

        /**
         * Returns a VLSN is equal to or smaller than that of all entries in
         * the queue. If the queue is empty, it returns
         * {@link OpenTransactionBuffer#MAX_VLSN}.
         */
        long getMinVLSNInQueue() {
            synchronized (boundedQueue) {
                return boundedQueue.stream().mapToLong(s -> {
                    final StreamSequenceId seq =
                        (StreamSequenceId) s.getSequenceId();
                    return seq.getSequence();
                }).min().orElse(OpenTransactionBuffer.MAX_VLSN);
            }
        }

        long getCurrSizeBytes() {
            return currSizeBytes;
        }

        long getCurrNumQueuedOps() {
            return boundedQueue.size();
        }

        Object[] getQueuedOps() {
            return boundedQueue.toArray();
        }

        long getMaxSizeBytes() {
            return maxSizeBytes;
        }

        /**
         * unit test only
         */
        public BlockingQueue<StreamOperation> getBoundedQueue() {
            return boundedQueue;
        }

        private boolean overflow(long sz) {
            return sz + currSizeBytes > maxSizeBytes;
        }

        private long getSizeBytes(StreamOperation op) {
            final StreamOperation.Type type = op.getType();
            if (StreamOperation.Type.PUT.equals(type)) {
                final StreamOperation.PutEvent put = op.asPut();
                return put.getRowSize();
            }
            if (StreamOperation.Type.DELETE.equals(type)) {
                final StreamOperation.DeleteEvent del = op.asDelete();
                return del.getPrimaryKeySize();
            }

            /* for other types, ignore the size */
            return 0;
        }

        private String lm(String msg) {
            return "[PU-Queue-" + ckptTableName + "-" + sid + "] " + msg;
        }
    }
}
