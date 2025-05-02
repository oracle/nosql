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
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Logger;

import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.security.StreamClientAuthHandler;
import oracle.kv.impl.rep.migration.generation.PartitionGenDBManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.server.JENotifyHooks;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.ShardTimeoutException;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.pubsub.SubscriptionInsufficientLogException;

import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.stream.BaseProtocol;
import com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType;
import com.sleepycat.je.rep.stream.FeederFilterChange;
import com.sleepycat.je.rep.stream.FeederFilterChangeResult;
import com.sleepycat.je.rep.subscription.Subscription;
import com.sleepycat.je.rep.subscription.SubscriptionAuthHandler;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;
import com.sleepycat.je.rep.subscription.SubscriptionStat;
import com.sleepycat.je.rep.subscription.SubscriptionStatus;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * Object that represents a client to consume replication stream from source
 * kvstore.
 */
public class ReplicationStreamConsumer {

    /* local address used in unit test only */
    static final String LOCAL_ADDRESS_IN_TEST = new HostPort(
        SubscriptionConfig.ANY_ADDRESS.getHostName(), 65535).toString();

    /* min ha protocol version which server should support */
    private static final int MIN_HA_PROTOCOL_VERSION = BaseProtocol.VERSION_7;

    /* subscription client uses external node type to connect feeder */
    private static final NodeType CLIENT_NODE_TYPE = NodeType.EXTERNAL;
    /* subscription client id starts with a fixed prefix */
    private static final String CONSUMER_ID_PREFIX = "RSC";
    /* statistics collection internal in ms */
    private static final int STAT_COLL_INTERVAL_MS = 1000;
    /* monitoring interval in ms */
    private static final long MONITORING_INTERVAL_MS = 100;
    /* onWarning signal interval in ms */
    private static final long SIGNAL_WARN_INTERVAL_MS = 1000 * 60 * 60;

    /* private logger */
    private final Logger logger;
    /* consumer id */
    private final String consumerId;
    /* parent publisher */
    private final PublishingUnit pu;
    /* replication group id of the source feeder */
    private final RepGroupId repGroupId;
    /* txn buffer for replication stream */
    private final OpenTransactionBuffer txnBuffer;
    /* monitoring thread */
    private final RSCMonitorThread monitorThread;
    /* directory used this consumer */
    private final String directory;
    /* statistics */
    private final ReplicationStreamConsumerStat stat;
    /* rep stream cbk */
    private final ReplicationStreamCbk replicationStreamCbk;
    /* true if the consumer has been shutdown */
    private final AtomicBoolean canceled;
    /* max number of reconnect attempts on error */
    private final long maxReconnect;

    /* JE subscription configuration used in streaming */
    private volatile SubscriptionConfig subscriptionConfig;
    /* feeder host and port, updated when master migrates */
    private volatile ShardMasterInfo master;
    /* subscription client of replication stream */
    private volatile Subscription subscriptionClient;
    /*
     * True if the shard is a new born shard without owning partition, false
     * if the shard at least owns one partition.
     *
     * In store expansion, a new born shard may not own any partition, it
     * need to be distinguished from a retired shard that all partitions on
     * that shard have migrated out, in order to avoid that the RSC to the new
     * born shard is killed by PU because PU incorrectly treats it a retired
     * shard.
     */
    private volatile boolean newBornShard;
    /*
     * True if all partitions in the shard have been closed and streamed,
     * false otherwise.
     * If all partitions are closed and streamed, the RSC may be closed since
     * there is no open generation to stream from the shard. Once it is set
     * to true, the RSC should be closed and the flag cannot return to false
     */
    private volatile boolean allPartClosed;

    /* reauthentication handler */
    private final StreamClientAuthHandler authHandler;
    /* security properties */
    private final Properties securityProps;

    /* For unit tests */
    private volatile TestHook<NoSQLStreamMode> ileHook = null;

    private final NoSQLStreamMode mode;

    /* tables currently being subscribed, empty if all tables subscribed */
    private final Set<TableImpl> tables;
    /* feeder filter used in reconnect, null if filter is not enabled */
    private final NoSQLStreamFeederFilter feederFilter;

    /** logging handler */
    private final Handler logHandler;

    /**
     * Creates a data consumer and initialize internal data structures and
     * start worker thread.
     *
     * @param pu                parent publishing unit
     * @param master            feeder node id and its host port pair
     * @param repGroupId        id of feeder's replication group
     * @param outputQueue       queue for all output messages
     * @param subscribedTables  subscribed tables, null if all tables
     * @param rootDir           root directory
     * @param feederFilter      subscription feeder filter
     * @param mode              stream mode
     * @param maxReconnect      max number of reconnect attempts on error
     * @param deserializer      StreamOperation deserializer
     * @param securityProps     properties for secure store, null for
     *                          non-secure store
     * @param logger            private logger
     *
     * @throws UnknownHostException if feeder node not accessible
     * @throws SubscriptionFailureException if invalid stream mode
     */
    public ReplicationStreamConsumer(PublishingUnit pu,
                                     ShardMasterInfo master,
                                     RepGroupId repGroupId,
                                     PublishingUnit.BoundedOutputQueue
                                         outputQueue,
                                     Collection<TableImpl> subscribedTables,
                                     String rootDir,
                                     NoSQLStreamFeederFilter feederFilter,
                                     NoSQLStreamMode mode,
                                     long maxReconnect,
                                     StreamOperation.Deserializer deserializer,
                                     Properties securityProps,
                                     Logger logger)
        throws UnknownHostException, SubscriptionFailureException {

        final String sid;
        if (pu == null) {
            /* unit test without pu */
            sid = "TestNoSQLSubscriber";
        } else {
            final String ckptTableName = pu.getCkptTableManager()
                                           .getCkptTableName();
            final String namespaceName =
                NameUtils.getNamespaceFromQualifiedName(ckptTableName);
            /*
             * node name does not support ":", thus build it separately.  Note
             * that namespace and table names cannot contain "-".
             */
            final String namespacePrefix =
                (namespaceName == null) ? "" : namespaceName + "-";
            /* get the external node name from pu */
            sid = namespacePrefix +
                  NameUtils.getFullNameFromQualifiedName(ckptTableName) +
                  "-" + pu.getSubscriberId();

        }

        consumerId = CONSUMER_ID_PREFIX + "-" + sid + "-" + repGroupId;

        this.pu = pu;
        this.master = master;
        this.repGroupId = repGroupId;
        this.logger = logger;
        this.securityProps = securityProps;
        this.maxReconnect = maxReconnect;
        this.mode = mode;
        canceled = new AtomicBoolean(false);
        logHandler = new JENotifyHooks.RedirectHandler(logger);
        if (feederFilter != null) {
            if (subscribedTables == null) {
                /* subscribe all tables */
                tables = null;
                logger.fine(() -> lm("To stream all user tables"));
            } else if (subscribedTables.isEmpty()) {
                /* subscribe no tables, empty stream */
                tables = new HashSet<>();
                logger.fine(() -> lm("Allow no user table, empty stream"));
            } else {
                /* subscriber some tables */
                tables = new HashSet<>(subscribedTables);
                logger.fine(() -> lm("To stream tables=" + tables));
            }
            this.feederFilter = feederFilter;
        } else {
            /* never used if no feeder filter */
            tables = null;
            this.feederFilter = null;
        }

        /*
         * TODO:
         * Now we need a writeable dir at client side, which is only used
         * to dump the traces. Elimination of this dependency requires
         * modification of JE subscription client.
         */
        directory = NoSQLPublisher.ensureDir(rootDir, consumerId, true);

        /* build JE subscription configuration */
        if (pu == null || pu.isNonSecureStore()) {
            authHandler = null;
            /* non-secure store */
            subscriptionConfig =
                new SubscriptionConfig(consumerId,
                                       directory,
                                       (pu == null) ?
                                           LOCAL_ADDRESS_IN_TEST :
                                           pu.getPublisherLocalAddress(),
                                       master.getMasterHostPort(),
                                       repGroupId.getGroupName(),
                                       null,
                                       CLIENT_NODE_TYPE);
        } else {
            /* secure store with authentication */
            final long reAuthInv = pu.getSubscriber().getSubscriptionConfig()
                                     .getReAuthIntervalMs();
            authHandler = StreamClientAuthHandler.getAuthHandler(
                new NameIdPair(consumerId), pu, reAuthInv, repGroupId, logger);

            subscriptionConfig =
                new SubscriptionConfig(consumerId,
                                       directory,
                                       pu.getPublisherLocalAddress(),
                                       master.getMasterHostPort(),
                                       repGroupId.getGroupName(),
                                       null,
                                       CLIENT_NODE_TYPE,
                                       authHandler,
                                       securityProps);
            logger.info(lm("To stream from a secure store, re-authentication " +
                           "interval(ms)=" + reAuthInv));
        }
        subscriptionConfig.setLoggingHandler(logHandler);

        /* set ha protocol version */
        subscriptionConfig.setMinProtocolVersion(MIN_HA_PROTOCOL_VERSION);

        /* set part md db name */
        subscriptionConfig.setPartGenDBName(PartitionGenDBManager.getDBName());

        /* set feeder filter */
        if (feederFilter != null) {
            subscriptionConfig.setFeederFilter(feederFilter);
        }

        /* build replication stream callback to replace default */
        final BlockingQueue<DataEntry> inputQueue =
            new ArrayBlockingQueue<>(subscriptionConfig
                                         .getInputMessageQueueSize());
        stat = new ReplicationStreamConsumerStat(this);
        replicationStreamCbk = new ReplicationStreamCbk(inputQueue, stat,
                                                        logger);
        subscriptionConfig.setCallback(replicationStreamCbk);

        /* set request type  */
        final EntryRequestType reqType;
        switch (mode) {
            case FROM_NOW:
                reqType = EntryRequestType.NOW;
                break;

            case FROM_EXACT_STREAM_POSITION:
            case FROM_EXACT_CHECKPOINT:
                reqType = EntryRequestType.DEFAULT;
                break;

            case FROM_STREAM_POSITION:
            case FROM_CHECKPOINT:
                reqType = EntryRequestType.AVAILABLE;
                break;

            default:
                final NoSQLSubscriberId id =
                    (pu == null) ? null : pu.getSubscriberId();
                throw new SubscriptionFailureException(
                    id, "Invalid stream mode " + mode);
        }
        subscriptionConfig.setStreamMode(reqType);

        /* client to be built in startClient */
        subscriptionClient = null;

        /* false by default */
        newBornShard = false;
        allPartClosed = false;

        /* create txn agenda */
        txnBuffer = new OpenTransactionBuffer(this,
                                              repGroupId,
                                              inputQueue,
                                              outputQueue,
                                              subscribedTables,
                                              deserializer,
                                              logger);

        monitorThread = new RSCMonitorThread();
    }

    /**
     * In test only
     * <p>
     * Starts a consumer from the very beginning
     *
     * @throws InsufficientLogException  if source does not have the log to
     *                                   serve streaming from requested start
     *                                   vlsn
     * @throws InternalException         if other errors fail the subscription
     */
    public void start() throws InsufficientLogException, InternalException  {
        start(FIRST_VLSN);
    }

    /**
     * Starts a consumer from a specific VLSN
     *
     * @param startVLSN   start vlsn to stream from
     *
     * @throws InsufficientLogException if source does not have the log to
     * serve streaming from requested start vlsn.
     * @throws SubscriptionFailureException if other errors that fail the
     * subscription
     */
    public void start(long startVLSN)
        throws InsufficientLogException, SubscriptionFailureException {

        /* start all worker threads */
        logger.fine(() -> lm("Start RSC from vlsn=" +
                             (startVLSN == Long.MAX_VALUE ? "<now>" :
                                 startVLSN) + ")" +
                             " with request entry type=" +
                             subscriptionConfig.getStreamMode()));

        /* start subscription client, null vlsn check must be done  */
        try {
            subscriptionClient = startClient(startVLSN, subscriptionConfig);
            txnBuffer.startWorker();

            /* start monitoring */
            monitorThread.start();
            logger.fine(() -> lm("Subscription client starts from VLSN=" +
                                 subscriptionClient.getStatistics()
                                                   .getStartVLSN() +
                                 " (req: " + (startVLSN == Long.MAX_VALUE ?
                                     "<now>" : startVLSN) + ")" +
                                 " from node=" + master.getMasterRepNodeId()));

        } catch (InsufficientLogException ile) {
            /* requested VLSN is not available, switch to partition transfer */
            logger.info(lm("Requested VLSN=" + startVLSN +
                           " is not available at node=" +
                           master.getMasterRepNodeId()));

            /*
             * just throw it to PU, PU will cancel the whole subscription,
             * shut down all RSC, and signal subscriber
             */
            throw ile;
        } catch (IllegalArgumentException | GroupShutdownException |
            InternalException | TimeoutException |
            ReplicationSecurityException cause) {

            final String err = "Unable to start streaming from node=" +
                               master.getMasterRepNodeId();
            logger.warning(lm(err + ", error=" + cause));

            /*
             * just throw it to PU, PU will cancel the whole subscription,
             * shut down all RSC, and signal subscriber
             */
            final NoSQLSubscriberId sid = (pu == null) ?
                new NoSQLSubscriberId(1, 0) : pu.getSubscriberId();
            throw new SubscriptionFailureException(
                sid, err + ", error=" + exceptionString(cause), cause);
        }
    }

    /**
     * Stops a replication stream client.
     *
     * @param logStat  true if dump stat in log
     */
    void cancel(boolean logStat) {

        /* avoid concurrent and recursive stop calls */
        if (!canceled.compareAndSet(false, true)) {
            return;
        }

        /* shutdown monitor before shutting down client */
        monitorThread.shutdownThread(logger);
        logger.fine(() -> lm("monitor thread has shutdown."));

        /* shutdown otb */
        txnBuffer.close();
        logger.fine(() -> lm("OTB has shutdown."));

        /* shutdown subscription client */
        if (subscriptionClient != null) {
            subscriptionClient.shutdown();
        }
        logger.fine(() -> lm("subscription client has shutdown."));

        if (logStat) {
            logger.fine(() -> lm("stats=" + stat.dumpStat()));
        }

        logger.fine(() -> lm("RSC has shut down"));
    }

    /**
     * Returns true if the shard is a new born shard, false otherwise.
     *
     * @return Returns true if the shard is a new born shard, false otherwise
     */
    boolean isNewBornShard() {
        return newBornShard;
    }

    /**
     * Sets the shard as a new born shard
     */
    void setNewBornShard() {
        newBornShard = true;
        logger.info(lm("RSC is set as to a new born shard=" + repGroupId));
    }

    /**
     * Clears the flag that the shard as a new born shard
     */
    void clearNewBornShard() {
        newBornShard = false;
        logger.info(lm("RSC is cleared as to a new born shard=" + repGroupId));
    }

    /**
     * Returns true if all partitions in the shard have been closed and
     * streamed, false otherwise.
     *
     * @return true if all partitions in the shard have been closed and
     * streamed, false otherwise.
     */
    boolean isAllPartClosed() {
        return allPartClosed;
    }

    /**
     * Sets that all partitions have closed and streamed
     */
    void setAllPartClosed() {
        allPartClosed = true;
    }

    /**
     * Gets the statistics of the consumer
     *
     * @return  the statistics of the consumer
     */
    ReplicationStreamConsumerStat getRSCStat() {
        return stat;
    }

    @Override
    public String toString() {
        return "RSC: [" + "shard=" + repGroupId + ", " +
               "source node=" + master.getMasterRepNodeId() + "\n" +
               "source shard=" + master.getShardId() + "\n" +
               "source HA addr=" + master.getMasterHostPort() + "\n" +
               stat.dumpStat();
    }

    /* for test use only */
    ReplicationStreamCbk getRepStrCbk() {
        return replicationStreamCbk;
    }

    /**
     * Returns the replication group id for this consumer
     *
     * @return the replication group id for this consumer
     */
    public RepGroupId getRepGroupId() {
        return repGroupId;
    }

    /**
     * Returns consumer id
     *
     * @return consumer id
     */
    public String getConsumerId() {
        return consumerId;
    }

    /**
     * Gets open txn buffer
     *
     * @return open txn buffer
     */
    public OpenTransactionBuffer getTxnBuffer() {
        return txnBuffer;
    }

    /**
     * Gets parent publishing unit.
     *
     * @return  parent PU, or null in certain unit tests where no publishing
     * unit is created.
     */
    PublishingUnit getPu() {
        return pu;
    }

    /* unit test only */
    void setILEHook(TestHook<NoSQLStreamMode> hook) {
        ileHook = hook;
    }

    /**
     * Gets subscriber authentication handler
     *
     * @return subscriber authentication handler, null when no authentication
     * handler is needed for non-secure store.
     */
    SubscriptionAuthHandler getAuthHandler() {
        return authHandler;
    }

    /**
     * unit test only
     */
    public void setPGMProcWaitTimeout(int waitTimeoutMS) {
        if (txnBuffer == null) {
            return;
        }

        txnBuffer.getPartGenMarkProcessor().setWaitTimeout(waitTimeoutMS);
    }

    /**
     * Unit test only
     */
    public Set<TableImpl> getCachedTables() {
        return tables;
    }

    /**
     * Applies the change
     *
     * @param changeThread    change thread
     * @param type      type of the change
     * @param tableImpl table instance
     *
     * @return subscription change result
     */
    ChangeStreamShardResult applyChange(StoppableThread changeThread,
                                        StreamChangeReq.Type type,
                                        TableImpl tableImpl) {

        if (tableImpl == null) {
            final String msg = "Table cannot be null in changing stream";
            logger.warning(lm(msg));
            throw new IllegalArgumentException(msg);
        }
        /* wildcard stream cannot be modified, thus it cannot be null */
        if (tables == null) {
            final String err = "Wildcard stream cannot be modified" +
                               ", type=" + type + ", table=" +
                               tableImpl.getFullNamespaceName();
            logger.warning(lm(err));
            throw new IllegalStateException(err);
        }

        final String tableName = tableImpl.getFullName();
        final int gid  = repGroupId.getGroupId();
        if (pu == null) {
            /* in unit test only */
            return new ChangeStreamShardResult(tableName, gid,
                new IllegalStateException("In a unit test without PU."));
        }

        if (subscriptionClient == null || canceled.get()) {
            return new ChangeStreamShardResult(tableName, gid,
                new IllegalStateException("Stream " + consumerId +
                                          " to shard " + repGroupId +
                                          ", null client=" +
                                          (subscriptionClient == null) +
                                          ", canceled=" + canceled.get()));
        }

        final NoSQLStreamFeederFilter.MatchKey matchKey =
            new NoSQLStreamFeederFilter.MatchKey(tableImpl);
        final String reqId = getStreamChangeReqId();

        /* send change filter request */
        final FeederFilterChange change;

        if (type.equals(StreamChangeReq.Type.ADD)) {
            change = new StreamChangeSubscribeReq(reqId,
                                                  tableName,
                                                  matchKey.rootTableId,
                                                  matchKey.getTableId(),
                                                  matchKey.keyCount,
                                                  matchKey.skipCount,
                                                  logger);
        } else if (type.equals(StreamChangeReq.Type.REMOVE)) {
            change = new StreamChangeUnsubscribeReq(reqId,
                                                    tableName,
                                                    matchKey.rootTableId,
                                                    matchKey.getTableId(),
                                                    logger);
        } else {
            throw new IllegalStateException("Unsupported change request" +
                                            " type " + type);
        }

        final ChangeStreamShardResult ret = sendChangeReq(changeThread,
                                                          tableName,
                                                          type,
                                                          change);

        if (ret.getResult() == null) {
            logger.info(lm("Change (table=" + tableName +
                           ", type=" + type +
                           ") timeout or discarded."));
        } else if (ret.getResult().getStatus()
               .equals(FeederFilterChangeResult.Status.OK)) {

            /* after successful filter change, update set of cached tables */
            if (type.equals(StreamChangeReq.Type.ADD)) {
                tables.add(tableImpl);
            } else {
                tables.remove(tableImpl);
            }
            logger.info(lm("[ReqId=" + change.getReqId() + "] " +
                           "Change applied to filter successfully"));
        } else if (ret.getResult().getStatus()
                      .equals(FeederFilterChangeResult.Status.NOT_APPLICABLE)) {
            logger.info(lm("[ReqId=" + change.getReqId() + "] " +
                           "Change cannot be applied to filter"));
        } else if (ret.getResult().getStatus()
                      .equals(FeederFilterChangeResult.Status.FAIL)) {
            logger.info(lm("[ReqId=" + change.getReqId() + "] " +
                           "Fail to apply change"));
        } else {
            throw new IllegalStateException(
                "[ReqId=" + change.getReqId() + "] " +
                "Unsupported status=" + ret.getResult().getStatus());
        }

        return ret;
    }

    void addCachedTable(String rootTblId, TableImpl tableImpl) {
        txnBuffer.addCachedTable(rootTblId, tableImpl);
    }

    void removeCachedTable(String rootTblId, String tblId) {
        txnBuffer.removeCachedTable(rootTblId, tblId);
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private long getChangeTimeoutMs() {
        return (pu != null ? getPu().getChangeTimeoutMs() :
            60 * 1000 /*  unit test */);
    }

    private ChangeStreamShardResult sendChangeReq(StoppableThread changeThread,
                                                  String tableName,
                                                  StreamChangeReq.Type type,
                                                  FeederFilterChange change) {
        logger.info(lm("[ReqId=" + change.getReqId() + "] " +
                       "Send change request for table=" + tableName +
                       ", timeout in ms=" + getChangeTimeoutMs() +
                       ", type=" + type + ", table=" + tableName +
                       ", change request=" + change));

        final int gid  = repGroupId.getGroupId();
        final String reqId = change.getReqId();
        final StreamChangeResultHandler rh =
            new StreamChangeResultHandler(changeThread, repGroupId,
                                          (StreamChangeReq) change, logger);

        /*
         * check if change thread has shutdown, if so, instantly returns
         * without sending the request.
         */
        if (changeThread.isShutdown()) {
            final String err = "[ReqId=" + reqId + "] " +
                               "Change thread to has shutdown, no need to " +
                               "send request for table=" + tableName;
            final IllegalStateException ise = new IllegalStateException(err);
            logger.info(lm(err));
            return new ChangeStreamShardResult(tableName, gid, ise);
        }

        try {
            subscriptionClient.changeFilter(change, rh);
        } catch (Exception exp) {
            final String err = "[ReqId=" + reqId + "] " +
                               "Cannot apply change=" + change + " to shard=" +
                               gid + ", reason=" + exp;
            logger.warning(lm(err));
            return new ChangeStreamShardResult(tableName, gid, exp);
        }

        /* wait for result */
        final long changeTimeoutMs = getChangeTimeoutMs();
        final FeederFilterChangeResult result;
        try {
            result = rh.getResult(changeTimeoutMs);
        } catch (TimeoutException | InterruptedException te) {
            logger.warning(lm("[ReqId=" + reqId + "] " +
                              "Fail to get change result, " +
                              ", type=" + type + ", table=" + tableName +
                              ", error=" + te));
            return new ChangeStreamShardResult(tableName, gid, te);
        }

        /* get result from feeder */
        return new ChangeStreamShardResult(tableName, gid, result);
    }

    private String getStreamChangeReqId() {
        return UUID.randomUUID().toString().substring(0, 7);
    }

    private String lm(String msg) {
        return "[" + consumerId + "] " + msg;
    }

    /* start subscription client */
    private Subscription startClient(long startVLSN,
                                     SubscriptionConfig conf)
        throws InsufficientLogException, TimeoutException,
        ReplicationSecurityException {

        /* for unit test, throw ILE */
        assert TestHookExecute.doHookIfSet(ileHook, mode);

        /* build new replication stream client */
        final Subscription client = new Subscription(conf, logger);

        client.start(startVLSN);

        stat.setReqStartVLSN(startVLSN);
        stat.setAckedStartVLSN(client.getStatistics().getStartVLSN());

        /* remember partition md db id for later use */
        stat.setPartGenDBId(client.getStatistics().getPartGenDBId());
        if(stat.getPartGenDBId() == null) {
           logger.warning(lm("Database id of partition generation is null"));
        } else {
            logger.info(lm("Set database id of partition generation db" +
                           ", id=" + stat.getPartGenDBId()));
        }

        /* if actual vlsn is later than requested start vlsn, checkpoint */
        if (stat.getAckedStartVLSN() > stat.getReqStartVLSN() &&
            pu != null && pu.isCkptEnabled()) {
            try {
                final long ts = System.currentTimeMillis();
                final StreamPosition sp =
                    pu.getCkptTableManager().updateShardCkpt(
                        pu.getStoreName(), pu.getStoreId(), repGroupId,
                        stat.getAckedStartVLSN());
                logger.info(lm("Update checkpoint to pos=" + sp +
                               ", elapsed time(ms)=" +
                               (System.currentTimeMillis() - ts)));

            } catch (CheckpointFailureException cfe) {
                /*
                 * We can afford to miss a checkpoint here and no need to
                 * terminate the stream if checkpoint fails, because user
                 * can wait for the next checkpoint.
                 */
                logger.warning(lm("Fail to make a checkpoint for " +
                                  " actual start vlsn=" +
                                  stat.getAckedStartVLSN() +
                                  ", continue streaming"));
                /* signal user that checkpoint failed */
                try {
                    pu.getSubscriber().onWarn(cfe);
                } catch (Exception exp) {
                    logger.warning(lm("Exception in executing " +
                                      "subscriber's onWarn()=" +
                                       exp + "\n" +
                                       LoggerUtils.getStackTrace(exp)));
                }
            }
        }
        return client;
    }

    /*
     * Private monitoring thread to keep track of subscription client status.
     *
     * If the subscription client fails, the monitor thread should analyze the
     * failure and retry if possible. For example, during master migration,
     * the subscription client will fail due to connection error, the
     * monitoring thread should obtain the new master from publisher and
     * restart the subscription client with new master.
     */
    private class RSCMonitorThread extends StoppableThread {

        /*
         * soft shutdown timeout in ms, monitor thread may busy restarting
         * or closing an underlying subscription client. Currently in JE
         * subscription API, each retry would be after 1 second sleep and
         * there are 3 retries at most by default, hence we set the soft
         * shutdown timeout to 5 seconds to give it some room to exit gracefully
         */
        private static final int THREAD_SOFT_SHUTDOWN_MS = 5000;

        /* stat update interval */
        private final long collIntvMs;
        /* shard timeout */
        private final long timeoutInMs;
        /* warning signal interval */
        private final long warningIntvMs;

        private int numAttempts;
        private int numSucc;
        private long lastStatCollectTimeMs;
        private long lastWarningTimeMs;

        RSCMonitorThread() {
            super("RSC-monitor-" + consumerId);

            collIntvMs = STAT_COLL_INTERVAL_MS;
            warningIntvMs = SIGNAL_WARN_INTERVAL_MS;

            if (pu == null) {
                /* unit test only */
                timeoutInMs = 60 * 1000;
            } else {
                timeoutInMs = pu.getShardTimeoutMs();
            }

            numAttempts = 0;
            numSucc = 0;
            lastStatCollectTimeMs = 0;
        }

        @Override
        public void run() {

            logger.fine(() -> lm("Monitor thread starts."));

            Throwable cause = null;
            try {
                /* loop until thread shutdown or throw sfe or sile */
                while (!isShutdown()) {

                    updateStatistics();

                    final SubscriptionStatus status =
                        subscriptionClient.getSubscriptionStatus();
                    switch (status) {
                        case INIT:
                        case SUCCESS:
                            break;
                        case CONNECTION_ERROR:
                        case UNKNOWN_ERROR:
                            if (pu.isClosed()) {
                                /*
                                 * error because PU closed, no retry. monitor
                                 * will be shutdown
                                 */
                                continue;
                            }

                            if (subscriptionClient != null) {
                                subscriptionClient.shutdown();
                                logger.fine(
                                    () -> lm("subscription client shutdown, " +
                                             "status=" + status +
                                             ", statistics=" +
                                             dump(subscriptionClient
                                                      .getStatistics())));
                            }

                            /* ask PU where we should start in reconnect */
                            final long vlsn = pu.getReconnectVLSN(
                                repGroupId.getGroupId());
                            /* remember where we stop */
                            stat.setLastVLSNBeforeReconnect(vlsn);
                            handleErrorWithRetry(vlsn);
                            break;

                        case SECURITY_CHECK_ERROR:
                            /* no retry if security error */
                            if (subscriptionClient != null) {
                                subscriptionClient.shutdown();
                            }

                            /* get cause of failure to pass to SFE */

                            /* first check subscription client */
                            Throwable exp =
                                subscriptionClient.getStoredException();
                            if (exp == null) {
                                /* check reauthentication */
                                exp = authHandler.getCause();
                            }

                            final String err =
                                "Security check failed, shutdown without " +
                                "retry. Cause of failure=";

                            logger.warning(lm(err + exp));
                            /* create SFE with cause of failure */
                            throw new SubscriptionFailureException(
                                pu.getSubscriberId(),
                                err + exceptionString(exp),
                                exp);

                        default:
                    }

                    synchronized (this) {
                        this.wait(MONITORING_INTERVAL_MS);
                    }
                }

            } catch (InterruptedException ie) {
                cause = ie;
                /* rsc requires to shut down monitor, no need to escalate */
                logger.fine(() -> lm("Thread-" + getName() + " is " +
                                     "interrupted and exists."));
            } catch (SubscriptionInsufficientLogException sile) {
                cause = sile;
                logger.warning(lm("Unable to restart subscription client due " +
                                 "to insufficient log at server for " +
                                  "subscriber id=" + sile.getSubscriberId() +
                                  ", in shard=" + repGroupId +
                                  ", requested vlsn=" +
                                  sile.getReqVLSN(repGroupId)));
            } catch (SubscriptionFailureException e) {
                cause = e;
                logger.warning(lm("Unable to restart subscription client" +
                                  " reason=" + e));
            } catch (RuntimeException re) {
                /* Other runtime exception, dump the stack */
                cause = re;
                logger.warning(lm("Unexpected runtime error reason=" + re) +
                               "\n" + LoggerUtils.getStackTrace(re));
            } finally {

                /*
                 * if cause is not null, the RSC encounters an irrecoverable
                 * error and we need shut down the whole subscription, rather
                 * than shut down this particular RSC.
                 *
                 * Let PU close the whole subscription, and signal subscriber.
                 *
                 * PU will close each RSC when closing the subscription.
                 *
                 * In some unit tests, there is no PU.
                 */
                if (pu != null && cause != null) {
                    pu.close(cause);
                    final Throwable err = cause;
                    logger.fine(() -> lm("PU closed and monitor thread exits " +
                                         "with " + "error=" + err +
                                         " during lifetime, # attempted " +
                                         "connect=" + numAttempts +
                                         ", # successful connects=" + numSucc));
                } else {
                    /* normal RSC shutdown without closing PU, or no PU  */
                    logger.fine(() -> lm("Monitor thread exits, during " +
                                         "lifetime, # attempted connect=" +
                                         numAttempts +
                                         ", # successful connects=" + numSucc));
                }
            }
        }

        @Override
        public int initiateSoftShutdown() {
            /* wake up the thread and give it a bit time to exit */
            synchronized (this) {
                this.notify();
            }
            final boolean alreadySet = shutdownDone(logger);
            final int waitMs = THREAD_SOFT_SHUTDOWN_MS;
            logger.fine(() -> lm("Signal RSC monitor thread to shutdown, " +
                                 "shutdown already signalled?=" + alreadySet +
                                 "waitMs=" + waitMs));
            return waitMs;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }

        private String lm(String msg) {
            return "[RSC-MON-" + consumerId + "] " + msg;
        }

        private String dump(SubscriptionStat s) {
            return "[" +
                   "start vlsn=" + s.getStartVLSN() +
                   ", high vlsn=" + s.getHighVLSN() +
                   ", # msg received=" + s.getNumMsgReceived() +
                   ", # msg responded=" + s.getNumMsgResponded() +
                   ", # ops processed=" + s.getNumOpsProcessed() +
                   ", # txn committed=" + s.getNumTxnCommitted() +
                   ", # txn aborted=" + s.getNumTxnAborted()  +
                   "]";
        }

        /*
         * Monitor thread retry to create a stream to server after it gets a
         * new master HA from publisher.
         *
         * @param vlsn   start vlsn
         *
         * @throws SubscriptionInsufficientLogException
         * @throws SubscriptionFailureException
         */
        private void handleErrorWithRetry(long startVLSN)
            throws SubscriptionInsufficientLogException,
            SubscriptionFailureException {


            numAttempts++;
            logger.info(lm("Restart subscription client (# attempts=" +
                           numAttempts + ") from vlsn=" + startVLSN));

            /*
             * first shut down txn buffer worker thread and stop all activity,
             * and get the last committed VLSN the txn buffer has processed,
             * use that VLSN as the start stream point of new client.
             */
            txnBuffer.close();

            /* txn buffer ready, waiting for client to stream from feeder */
            int numRetry = 0;
            while (!canceled.get()) {
                try {

                    numRetry++;
                    if (numRetry <= maxReconnect) {
                        final int retry = numRetry;
                        logger.fine(() -> lm("Attempt (#=" + numAttempts +
                                             ") will restart client: " +
                                             "# of retry=" + retry +
                                             ", limit=" + maxReconnect));
                    } else {
                        final String err =
                            "Attempt (#=" + numAttempts + ")" +
                            "fails to start client after trying #times=" +
                            maxReconnect + ", throw " +
                            "SubscriptionFailureException to " +
                            "terminate subscription.";
                        logger.info(lm(err));
                        throw new SubscriptionFailureException(
                            pu.getSubscriberId(), err);
                    }

                    signalOnWarn(System.currentTimeMillis());

                    /* refresh to a new master HA host port */
                    synchronized (this) {
                        /* update volatile variable atomically */
                        master = pu.getMasterInfo(repGroupId, master);
                    }

                    if (master == null) {
                        final String err = "Subscriber= " +
                                           pu.getSubscriberId() +
                                           " is unable to get any master " +
                                           "HA for shard=" + repGroupId;

                        logger.warning(lm(err));

                        /*
                         * Check if the shard is still in topology. If it is,
                         * this shard is a valid shard but we are unable to
                         * get its master. In this case we need throw
                         * exception to caller.
                         *
                         * If it is not, this shard has been removed from the
                         * topology, probably due to contraction. In this
                         * case, we do not need to retry since this shard is
                         * no longer a valid shard, and we just cancel the
                         * RSC and remove it from parent PU. Monitor thread
                         * itself will exit after shutting down RSC.
                         */
                        final Topology topo = pu.getParent()
                                                .getPublisherTopoManager()
                                                .getTopology();
                        final RepGroupId gid = getRepGroupId();
                        if (!topo.getRepGroupIds().contains(gid)) {
                            /*
                             * The shard must have been removed without our
                             * noticing it through the normal closed generation
                             * notification we usually get through the stream.
                             * Just shut down this RSC rather than throwing an
                             * exception to avoid causing trouble for the rest
                             * of the stream.
                             */
                            logger.fine(() -> lm("Shard=" + gid + " no longer" +
                                                 " in the topology, skip " +
                                                 "reconnect, shut down, and " +
                                                 "remove RSC from parent PU."));

                            /* cancel RSC other than the monitor itself */
                            cancel(true);

                            /* remove RSC from PU */
                            ReplicationStreamConsumer.this
                                .getPu().getConsumers().remove(gid);

                            /*
                             * finally let main loop of monitor thread exit,
                             * it wont wait since called from within the
                             * monitor thread.
                             */
                            shutdownDone(logger);

                            /* return the caller and exit */
                            return;
                        }

                        /*
                         * In all other cases, the exception will be caught by
                         * the caller and the operation will be retried
                         */
                        throw new UnknownHostException(err);
                    }

                    final String feederHostPort = master.getMasterHostPort();
                    logger.fine(() -> lm("refreshed master HA for group=" +
                                         repGroupId + ", master=" +
                                         master.getMasterRepNodeId() +
                                         ", HA=" + feederHostPort));

                    /* build a new config with new feeder host port */
                    if (pu.isNonSecureStore()) {
                        /* unit test or non-secure store */
                        subscriptionConfig = new SubscriptionConfig(
                            consumerId, directory,
                            pu.getPublisherLocalAddress(),
                            feederHostPort, repGroupId.getGroupName(), null,
                            CLIENT_NODE_TYPE);
                    } else {
                        /* secure store */
                        subscriptionConfig = new SubscriptionConfig(
                            consumerId, directory,
                            pu.getPublisherLocalAddress(),
                            feederHostPort, repGroupId.getGroupName(), null,
                            CLIENT_NODE_TYPE, authHandler, securityProps);
                    }
                    subscriptionConfig.setLoggingHandler(logHandler);

                    /* set a updated feeder filter in reconnect */
                    if (feederFilter != null) {
                        subscriptionConfig.setFeederFilter(
                            feederFilter.updateFilter(tables));
                    }
                    subscriptionConfig.setCallback(replicationStreamCbk);

                    subscriptionClient = startClient(startVLSN,
                                                     subscriptionConfig);

                    logger.info(lm("Subscriber=" + pu.getSubscriberId() +
                                   " creates a new client to shard=" +
                                   repGroupId + " to RN=" +
                                   master.getMasterRepNodeId() +
                                   " at host=" + feederHostPort +
                                   " last committed vlsn before reconnect=" +
                                   stat.getLastVLSNBeforeReconnect() +
                                   " and new stream will start from vlsn=" +
                                   startVLSN));

                    /* finally start txn buffer worker to consumer data */
                    txnBuffer.startWorker();
                    logger.info(lm("Transaction worker thread restarted."));

                    numSucc++;
                    return;

                } catch (GroupShutdownException cause) {
                    /* no need to retry */
                    final String err = "Subscriber=" + pu.getSubscriberId() +
                                       " is unable to start client from vlsn=" +
                                       startVLSN + " at " +
                                       ((master == null) ?
                                           " from unknown master " :
                                           " at " +
                                           master.getMasterRepNodeId());
                    logger.warning(lm(err + ", reason=" + cause));
                    throw new SubscriptionFailureException(
                        pu.getSubscriberId(),
                        err + ", reason=" + exceptionString(cause),
                        cause);
                } catch (InsufficientLogException ile) {
                    /* no enough logs, no need to retry */
                    final String err = "Subscriber=" + pu.getSubscriberId() +
                                       " is unable to start client " +
                                       "from vlsn " + startVLSN + " " +
                                       "due to insufficient log at shard=" +
                                       repGroupId + " of store=" +
                                       pu.getStoreName();
                    logger.warning(lm(err));

                    final SubscriptionInsufficientLogException sile =
                        new SubscriptionInsufficientLogException(
                            pu.getSubscriberId(), pu.getSubscribedTables(),
                            err);

                    sile.addInsufficientLogShard(repGroupId, startVLSN);

                    throw sile;

                } catch (UnknownHostException | IllegalArgumentException |
                    TimeoutException | InternalException  cause) {

                    /* retry on these exceptions */
                    final String err =
                        "Subscriber=" + pu.getSubscriberId() +
                        " is unable to start client from vlsn=" + startVLSN +
                        ((master == null) ? " from unknown master" :
                            " master=" + master.getMasterRepNodeId()) +
                        ", will refresh master and retry, " +
                        "cause=" + cause +
                        "\n" + LoggerUtils.getStackTrace(cause);

                    logger.warning(lm(err));
                }
            }
        }

        /* periodically update stats */
        private synchronized void updateStatistics() {

            final long curr = NoSQLSubscriptionImpl.getCurrTimeMs();
            if ((curr - lastStatCollectTimeMs) < collIntvMs) {
                return;
            }

            stat.setNumSuccReconn(numSucc);
            if (subscriptionClient != null) {
                /* collect feeder filter stat */
                final SubscriptionStat st = subscriptionClient.getStatistics();
                stat.setFeederFilterStat(st);
            }

            /* will signal onWarn if shard timeout */
            signalOnWarn(curr);

            /* update pu stat if pu exists, skip for unit test without pu */
            if (pu != null) {
                final SubscriptionStatImpl puStat =
                    (SubscriptionStatImpl) pu.getStatistics();
                puStat.updateShardStat(repGroupId, stat);
            }

            lastStatCollectTimeMs = curr;
        }

        private void signalOnWarn(long curr) {

            final long lastMsgTime = stat.getLastMsgTimeMs();
            if (!isShardDown(curr, lastMsgTime) || !needWarn(curr)) {
                return;
            }

            final String msg = "Shard=" + repGroupId.getGroupId() + " timeout";
            try {
                pu.getSubscriber().onWarn(
                    new ShardTimeoutException(repGroupId.getGroupId(),
                                              lastMsgTime,
                                              timeoutInMs,
                                              msg));
            } catch (Exception exp) {
                logger.warning(lm("Exception in executing " +
                                  "subscriber's onWarn()=" +
                                  exp + "\n" +
                                  LoggerUtils.getStackTrace(exp)));
            }


            lastWarningTimeMs = curr;
            logger.fine(() -> lm("Shard time out in streaming, " +
                                 "shard=" + repGroupId.getGroupId() +
                                 ", last msg time=" +
                                 FormatUtils.formatDateTimeMillis(lastMsgTime)));
        }

        private boolean isShardDown(long curr, long ts) {
            return (ts > 0) && ((curr - ts) > timeoutInMs);
        }

        private boolean needWarn(long curr) {
            return lastWarningTimeMs == 0 /* first warning */ ||
                   (curr - lastWarningTimeMs) > warningIntvMs;

        }
    }
}
