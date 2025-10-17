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

package oracle.kv.pubsub;

import static oracle.kv.KVSecurityConstants.AUTH_EXT_MECH_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_USERNAME_PROPERTY;
import static oracle.kv.KVSecurityConstants.KRB_MECH_NAME;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KerberosCredentials;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.pubsub.CheckpointTableManager;
import oracle.kv.impl.pubsub.PublisherTableMDManager;
import oracle.kv.impl.pubsub.PublisherTopoManager;
import oracle.kv.impl.pubsub.PublishingUnit;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Object represents a NoSQLPublisher which publishes changes to the NoSQL
 * store on order, an implementation of Reactive Streams Publisher interface.
 * <p>
 * For each kvstore, limit one instance of NoSQLPublisher running on a single
 * JVM. For repeated request of publisher with same configuration, the same
 * instance will be returned to user. If user requests a publisher instance
 * to a kvstore with existing publisher, but with different configuration
 * parameters, an exception will be raised to user. However, user is able to
 * change the parameters of an existing publisher.
 */
public class NoSQLPublisher implements Publisher<StreamOperation> {

    /*-----------------------*/
    /*- constant parameters -*/
    /*-----------------------*/

    /* publisher directory prefix */
    final static String PUBLISHER_DIR_PREFIX = "ondb_publisher_";

    /** Default maximum number of subscriptions. */
    public static final int DEFAULT_MAX_CONCURRENT_SUBS = 32;
    /** Default shard timeout in milliseconds. */
    public static final long DEFAULT_SHARD_TIMEOUT_MS = 10 * 60 * 1000;
    /** max retry in checking stream support */
    private static final int MAX_RETRY_STREAM_SUPPORT = 10;

    /* private logger */
    private final Logger logger;
    /* configuration to create this publisher */
    private final NoSQLPublisherConfig config;
    /* map between subscription to its publishing unit */
    private final ConcurrentMap<String, PublishingUnit> publishingUnitMap;
    /* If a shard is unreachable after this timeout period, signal onWarn() */
    private final long warningShardTimeoutMs;
    /* max number of concurrent subscriptions */
    private final long maxConcurrSubs;
    /* source kvstore name */
    private final String storeName;
    /* source kvstore id */
    private final long storeId;
    /* publisher root directory */
    private final String pubRootDir;
    /* handle to source kvstore */
    private final KVStore kvs;
    /* JE HA security property, null for non-secure store*/
    private final Properties jeHASecurityProp;
    /* true if the publisher has been shutdown  */
    private final AtomicBoolean closed;

    /* table md manager shared by all subscriptions */
    private final PublisherTableMDManager publisherTableMDManager;
    /* topology manager */
    private final PublisherTopoManager publisherTopoManager;

    /* publisher instance map keyed by publisher id */
    private final static ConcurrentMap<String, NoSQLPublisher> pubInstMap =
        new ConcurrentHashMap<>();

    /* security credentials, init after publisher is created */
    private final SecurityCred securityCred;

    /* id of publisher */
    private final String publisherId;

    /* only used internally when multi-publishers are on the same JVM */
    private static int suffix = 0;

    /*
     * number of subscriptions in initialization before starting stream,
     * incremented when init starts and decremented when it is over, either
     * succeed or fail.
     *
     * The total number of subscriptions in init plus the total number of
     * active subscriptions in pu map, should be always bounded by the max
     * number of concurrent subscriptions in publisher.
     *
     * Note this does not need to be volatile, as long as is only accessed
     * within the synchronization blocks in subscribe()
     */
    private int numInitSubscriptions;

    /*
     * unit test only, true use table name as id. In unt tests where table
     * created by TableBuilder set id 0 and table API encode table name in
     * the key bytes. The default is false;
     */
    private volatile boolean tableNameAsId = false;


    private NoSQLPublisher(NoSQLPublisherConfig config,
                           LoginCredentials loginCredentials,
                           boolean allowMultiPublisher,
                           Logger logger)
        throws PublisherFailureException {

        this.logger = logger;
        this.config = config;

        try {
            kvs = KVStoreFactory.getStore(config.getKvStoreConfig(),
                                          loginCredentials,
                                          config.getReauthHandler());
        } catch (Exception ex) {
            /* if fail to connect due to whatever reason,  throw pfe */
            final String msg = "Cannot connect Oracle NoSQL store=" +
                               config.getStoreName() +
                               ", error=" + exceptionString(ex);
            throw new PublisherFailureException(msg, true, ex);
        }

        /* check login credentials */
        if (loginCredentials != null &&
            (loginCredentials.getUsername() == null ||
             loginCredentials.getUsername().isEmpty())) {
                /*
                 * LoginCredentials implementation probably already check if
                 * user name is empty()
                 *
                 * For safety we double check to ensure user name to be
                 * non-empty in order to use Stream API
                 */
            throw new IllegalArgumentException(
                "Null or empty user name in login credentials.");
        }

        /* build security credentials */
        securityCred = buildSecurityCred(loginCredentials,
                                         config.getKvStoreConfig()
                                               .getSecurityProperties());
        if (securityCred.isSecureStore()) {
            jeHASecurityProp = config.getHaSecurityProperties();
        } else {
            jeHASecurityProp = null;
        }

        /* ensure root directory for publisher  */
        String pubDir = PUBLISHER_DIR_PREFIX + config.getStoreName();
        if (allowMultiPublisher) {
            /* used in unit test only */
            pubDir += suffix;
            suffix++;
        }

        try {
            pubRootDir = ensureDir(config.getRootPath(), pubDir,
                                   config.allowPreexistDir());
        } catch (Exception cause) {
            /* capture all exceptions and raise pfe to user */
            throw new PublisherFailureException("Unable to ensure the path to" +
                                                " publisher root directory, " +
                                                "cause: " +
                                                exceptionString(cause),
                                                false,
                                                cause);
        }

        storeName = ((KVStoreImpl) kvs).getTopology().getKVStoreName();
        storeId = ((KVStoreImpl) kvs).getTopology().getId();

        publisherTopoManager = new PublisherTopoManager(storeName, kvs, logger);
        publisherTableMDManager = new PublisherTableMDManager(storeName, kvs,
                                                              logger);
        warningShardTimeoutMs = config.getShardTimeoutMs();
        maxConcurrSubs = config.getMaxConcurrentSubs();
        publishingUnitMap = new ConcurrentHashMap<>();
        numInitSubscriptions = 0;
        closed = new AtomicBoolean(false);
        publisherId = makePublisherId(config.getPublisherId(), storeName);
        /* check store software edition */
        serverEditionCheck();
    }

    /**
     * Returns a handle to the NoSQL publisher after verifying that the store
     * exists and is accessible. An existing publisher instance will be
     * returned if possible. If security is configured with the system
     * properties, this method will return a NoSQL publisher to a secure store.
     * However, if no security is configured, this method will return a
     * NoSQL publisher for non-secure store.
     *
     * @param config          publisher configuration
     *
     * @return an instance of the publisher
     *
     * @throws PublisherFailureException if unable to create a publisher
     */
    public static NoSQLPublisher get(NoSQLPublisherConfig config) {

        final Logger logger = ClientLoggerUtils.getLogger(NoSQLPublisher.class,
                                                          "NoSQLPublisher");
        return NoSQLPublisher.get(config, null, logger);
    }

    /**
     * Returns a handle to the NoSQL publisher after verifying that the store
     * exists and is accessible. An existing publisher instance will be
     * returned if possible. If security is configured with the system
     * properties, this method will return a NoSQL publisher to a secure store.
     * However, if no security is configured, this method will return a
     * NoSQL publisher for non-secure store.
     *
     * @param config          publisher configuration
     * @param logger          private logger
     *
     * @return an instance of the publisher
     *
     * @throws PublisherFailureException if unable to create a publisher
     */
    public static NoSQLPublisher get(NoSQLPublisherConfig config,
                                     Logger logger)
        throws PublisherFailureException {
        return NoSQLPublisher.get(config, null, false, logger);
    }


    /**
     * Returns a handle to the NoSQL publisher after verifying that the store
     * exists and is accessible. An existing publisher instance will be
     * returned if possible.
     *
     * @param config          publisher configuration
     * @param loginCred       login credentials
     *
     * @return an instance of the publisher
     *
     * @throws PublisherFailureException if unable to create a publisher
     */
    public static NoSQLPublisher get(NoSQLPublisherConfig config,
                                     LoginCredentials loginCred) {

        /*
         * If a login credential is supplied, NoSQLPublisher uses the login
         * credential to verify store is accessible by the user. It does not
         * record it and it will be gone after the call returns.
         */
        final Logger logger = ClientLoggerUtils.getLogger(NoSQLPublisher.class,
                                                          "NoSQLPublisher");
        return NoSQLPublisher.get(config, loginCred, logger);
    }

    /**
     * Returns a handle to the NoSQL publisher after verifying that the store
     * exists and is accessible. An existing publisher instance will be
     * returned if possible.
     *
     * @param config          publisher configuration
     * @param loginCred       login credentials
     * @param logger          private logger
     *
     * @return an instance of the publisher
     *
     * @throws PublisherFailureException if unable to create a publisher
     */
    public static NoSQLPublisher get(NoSQLPublisherConfig config,
                                     LoginCredentials loginCred,
                                     Logger logger)
        throws PublisherFailureException {
        return NoSQLPublisher.get(config, loginCred, false, logger);
    }

    /**
     * @hidden
     *
     * Ensures a directory parent/child exists
     *
     * @param parent   parent directory
     * @param child    child directory
     * @param preExist true if OK to have a pre-exist directory
     * @return a full path of the directory parent/child
     * @throws IllegalArgumentException raised if directory cannot be ensured.
     */
    public static String ensureDir(String parent,
                                   String child,
                                   boolean preExist)
        throws IllegalArgumentException {

        final File dir = new File(parent, child);

        if (dir.exists()) {
            if (preExist) {
                return dir.getAbsolutePath();
            }
            /* pre-exist directory is not allowed */
            throw new IllegalArgumentException(
                "Pre-exist directory is not allowed: " + dir.getAbsolutePath());

        }

        /* create a directory */
        if (!dir.mkdir()) {
            throw new IllegalArgumentException("Cannot make directory " +
                                               dir.getAbsolutePath());
        }

        if (!dir.canWrite()) {
            throw new IllegalArgumentException("Cannot write directory " +
                                               dir.getAbsolutePath());
        }

        return dir.getAbsolutePath();
    }

    /**
     * @hidden
     *
     * Only used internally or in unit test
     * <p>
     * Returns a handle to the NoSQL publisher. Internally used or used in
     * unit test like scalable subscribers where the test need to create
     * multiple publishers to one kvstore on single JVM.
     *
     * @param config          publisher configuration
     * @param loginCred       login credentials
     * @param allowMultiPub   true if allow multi publishers to the same
     *                        store on single JVM
     * @param logger          private logger
     *
     * @return an instance of the publisher
     *
     * @throws PublisherFailureException if unable to create a publisher
     */
    public static NoSQLPublisher get(NoSQLPublisherConfig config,
                                     LoginCredentials loginCred,
                                     boolean allowMultiPub,
                                     Logger logger)
        throws PublisherFailureException {

        final String pubId = makePublisherId(config.getPublisherId(),
                                             config.getStoreName());
        NoSQLPublisher pub = pubInstMap.get(pubId);
        //TODO removing the allow multiple publisher since publihser id could
        // used to allow users to decide if multi-publisher against single
        // store is needed
        if (!allowMultiPub && pub != null && !pub.isClosed()) {

            if (pub.matchingPublisher(loginCred, config)) {
                /* return an existing instance */
                return pub;
            }

            throw new PublisherFailureException("A live NoSQLPublisher " +
                                                "instance to store " +
                                                config.getStoreName() +
                                                " already exists with " +
                                                "different parameters",
                                                false, null);
        }

        pub = new NoSQLPublisher(config, loginCred, allowMultiPub, logger);

        /*
         * Multiple-publishers to the same store on single JVM is only allowed
         * internally in unit test, in which we skip registering the publisher
         * into the map to avoid conflict. The suffix is still needed to
         * generate a unique publisher directory.
         */
        if (!allowMultiPub) {
            pubInstMap.put(pubId, pub);
        }

        return pub;
    }

    /**
     * @hidden
     *
     * Internal use only. Removes a publisher instance from the static map.
     * No effect if the instance does not exist
     * @param cf publisher configuration
     */
    public static NoSQLPublisher removePublisherInst(NoSQLPublisherConfig cf) {
        final String pubId = makePublisherId(cf.getPublisherId(),
                                             cf.getStoreName());
        return pubInstMap.remove(pubId);
    }

    /**
     * Creates a checkpoint table
     *
     * @param kvs               kvstore instance
     * @param ckptTableName     full namespace name of checkpoint table
     *
     * @throws PublisherFailureException if unable to create the checkpoint
     * table at kvstore.
     * @throws IllegalArgumentException if required parameter is invalid
     */
    static void createCheckpointTable(KVStore kvs, String ckptTableName)
        throws PublisherFailureException, IllegalArgumentException {

        if (kvs == null) {
            throw new IllegalArgumentException("KVStore instance cannot be " +
                                               "null");
        }

        if (ckptTableName == null || ckptTableName.isEmpty()) {
            throw new IllegalArgumentException(
                "Checkpoint table name cannot be null or empty");
        }

        try {
            CheckpointTableManager.createCkptTable(kvs, ckptTableName);
        } catch (Exception exp) {
            throw new PublisherFailureException(
                "Unable to create checkpoint table", true, exp);
        }
    }

    /**
     * @hidden
     *
     * Returns local address of the publisher
     *
     * @return local address of the publisher
     */
    public String getPublisherLocalAddress() {
        return config.getPublisherLocalAddress();
    }

    /**
     * A Subscriber establishes a NoSQL Subscription to initiate the flow of
     * elements from the Publisher. As part of the Subscription creation, the
     * subscription configuration is obtained via
     * {@link NoSQLSubscriber#getSubscriptionConfig()}. The configuration
     * contains information needed by the Publisher to start or resume the
     * stream. Normal completion of this method results in
     * {@link NoSQLSubscriber#onSubscribe}being invoked.
     *
     * <p>Each publisher can have at most one subscriber from a subscription
     * group specified in {@link NoSQLSubscriptionConfig}. For example, for a
     * subscription group with three subscribers, each must run in a separate
     * publisher, preferably on separate nodes. In other words, within each
     * Publisher, all concurrent subscriptions must be from distinct groups. If
     * a user tries to subscribe two subscriptions from the same group to same
     * Publisher, a {@link SubscriptionFailureException} will be raised.
     *
     * <p>Note his method is not allowed to throw any exceptions other than
     * NullPointerException.  All other exceptions must be delivered via
     * onError, according to rule Publisher.9 in reactive stream spec.
     *
     * @param s  subscriber trying to use the publisher
     *
     * @throws NullPointerException if subscriber is null
     */
    @Override
    public void subscribe(Subscriber<? super StreamOperation> s)
        throws NullPointerException {

        /*
         * From the reactive stream specification, Rule Publisher.9, NPE must
         * be signaled if subscriber is null.
         *
         * https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification
         */
        if (s == null) {
            throw new NullPointerException("Null subscriber for publisher to " +
                                           "kvstore " + getStoreName());
        }

        if (!(s instanceof NoSQLSubscriber)) {
            handleError(s, new IllegalArgumentException("Subscriber must be " +
                                                        "an instance of " +
                                                        "NoSQLSubscriber"));
            return;
        }

        final NoSQLSubscriber nosqlSub = (NoSQLSubscriber) s;
        if (closed.get()) {
            final PublisherFailureException pfe =
                new PublisherFailureException(
                    nosqlSub.getSubscriptionConfig().getSubscriberId(),
                    "publisher closed.",/* local */false, null);
            handleError(nosqlSub, pfe);
            return;
        }

        final NoSQLSubscriptionConfig conf = nosqlSub.getSubscriptionConfig();
        final NoSQLSubscriberId si = conf.getSubscriberId();
        if (conf.getUseExtCkptForElasticity()) {
            /*
             * ensure external checkpoint is implemented at the beginning,
             * avoid failing running streams during elastic operations
             */
            try {
                nosqlSub.getExternalLastCheckpoint();
                logger.info(lm("Stream is configured to use external " +
                               "checkpoint to handle elastic operations"));
            } catch (UnsupportedOperationException ue) {
                /* external checkpoint is configured but not implemented */
                final String err = "External checkpoint is not supported";
                logger.warning(lm(err + ", error=" + ue));
                final SubscriptionFailureException sfe =
                    new SubscriptionFailureException(si, err, ue);
                handleError(nosqlSub, sfe);
                return;
            }
        }

        /*
         * Per Rule Publisher.10 in the reactive stream spec, returns and
         * signal onError if duplicate subscription.
         */
        final PublishingUnit oldPU =
            publishingUnitMap.get(conf.getCkptTableName());

        /*
         * Any closed publishing unit stays in the map and can be queries for
         * statistics, until it is replaced by a new subscription.
         */
        if (oldPU != null && !oldPU.isClosed()) {
            /* exists a live subscription */
            final String err = "Duplicate subscription. Subscriber " +
                               conf.getSubscriberId() + ", tables: " +
                               conf.getTables();
            final SubscriptionFailureException sfe =
                new SubscriptionFailureException(si, err);
            handleError(nosqlSub, sfe);
            logger.warning(lm("Cannot subscribe a duplicate subscription, id=" +
                              conf.getSubscriberId() + ", tables=" +
                              conf.getTables()));
            return;
        }

        /* subscribers must not be more than shards */
        final Topology topo = getStoreTopology();
        if (si.getTotal() > topo.getRepGroupIds().size()) {
            handleError(nosqlSub,
                        new IllegalArgumentException(
                            "Total number of subscribers(" +
                            si.getTotal() + ") " +
                            "is more than shards in store " +
                            topo.getKVStoreName()));

            logger.warning(lm("Cannot subscribe because #subscribers=" +
                              si.getTotal() + " is more than #shards=" +
                              topo.getRepGroupIds().size()));
            return;
        }

        /*
         * finally check if we have enough room, need be synchronized among
         * all subscriptions in this publisher, that means initializations of
         * subscriptions are serialized in publisher.
         */
        synchronized (this) {
            if ((publishingUnitMap.size() + numInitSubscriptions) >=
                maxConcurrSubs) {

                /* running out of space */
                final PublisherFailureException pfe =
                    new PublisherFailureException(
                        nosqlSub.getSubscriptionConfig().getSubscriberId(),
                        "Publisher reached max subscriptions (" +
                        maxConcurrSubs +
                        "), currently active " + publishingUnitMap.size() +
                        ", in init " + numInitSubscriptions,
                        false, null);
                handleError(nosqlSub, pfe);
                logger.warning(lm("Has reached max concurrent subscriptions=" +
                                  maxConcurrSubs + ", currently #active=" +
                                  publishingUnitMap.size() + ", # in init=" +
                                  numInitSubscriptions));
                return;
            }

            /* still have room, take a slot and start initialization */
            numInitSubscriptions++;
        }

        /*
         * Make a subscription, if successful, a pu will be created and signal
         * subscriber via onSubscribe; if failed, pu should take care of all
         * exceptions and signal subscriber, so we do not expect to see any
         * exception raised from this call
         */
        try {
            PublishingUnit.doSubscribe(this, nosqlSub, logger);
        } finally {
            /*
             * either succeed or fail to init the PU, doSubscribe
             * should return and we decrement the counter here
             */
            synchronized (this) {
                numInitSubscriptions--;
            }
        }
    }

    /**
     * Closes a publisher without error, shut down all connection to source and
     * free resources. A closed publisher cannot be reopened, but user will be
     * able to create a new instance of NoSQLPublisher.
     *
     * @param delDir  true if delete the publisher root directory
     */
    public void close(boolean delDir) {
        close(null, delDir);
    }

    /**
     * Closes a publisher, shut down all connection to source and free
     * resources. A closed publisher cannot be reopened, but user will be
     * able to create a new instance of NoSQLPublisher.
     *
     * @param cause   null if normal close, otherwise it is the cause of
     *                failure that leads to publisher close.
     * @param delDir  true if delete the publisher root directory
     */
    public void close(Throwable cause, boolean delDir) {

        if (!closed.compareAndSet(false, true)) {
            return;
        }

        /*
         * From the reactive stream specification, Rule Publisher.5, a
         * publisher terminates successfully must signal onComplete to
         * subscriber
         *
         * https://github.com/reactive-streams/reactive-streams-jvm/tree/v1.0.0#specification
         *
         */
        for (final PublishingUnit pu : publishingUnitMap.values()) {

            /* Signal all live subscribers */
            if (!pu.isClosed()) {
                if (cause == null) {
                    /*
                     * Per Rule Publisher.5 in the spec, If a Publisher
                     * terminates successfully (finite stream) it MUST
                     * signal an onComplete.
                     */
                    pu.close(null);
                } else {
                    /* otherwise signal error and close pu */
                    final PublisherFailureException pfe =
                        new PublisherFailureException(
                            pu.getSubscriber()
                              .getSubscriptionConfig()
                              .getSubscriberId(),
                            "Publisher will close and need to shut down all " +
                            "subscribers", false, cause);

                    pu.close(pfe);
                }
            }
        }

        if (delDir) {
            FileUtils.deleteDirectory(new File(pubRootDir));
        }

        publishingUnitMap.clear();

        kvs.close();
        logger.fine(() -> lm("NoSQLPublisher closed " +
                             ((cause == null) ? "normally" :
                                 "with error=" + cause) +
                             ", publisher root directory (" + pubRootDir + ") " +
                             (delDir ? "deleted" : "kept for diagnosis")));
        if (cause != null) {
            logger.fine(() -> lm("Stack trace of failure:\n" +
                                 LoggerUtils.getStackTrace(cause)));
        }
    }

    /**
     * Gets kvstore store id
     *
     * @return kvstore store id
     */
    public long getStoreId() {
        return storeId;
    }

    /**
     * Gets the kvstore name
     *
     * @return kvstore name
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Returns true is the publisher has already shutdown
     *
     * @return true is the publisher has already shutdown
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * @hidden
     */
    public KVStore getKvs() {
        return kvs;
    }

    /**
     * @hidden
     */
    public long getDefaultShardTimeoutMs() {
        return warningShardTimeoutMs;
    }

    /**
     * @hidden
     */
    public String getPubRootDir() {
        return pubRootDir;
    }

    /**
     * @hidden
     *
     * Gets a handle to a remote kvstore
     *
     * @param storeName     name of kv store
     * @param helperHosts   list of helper hosts
     *
     * @return a handle of KVStoreImpl to remote kvstore
     *
     * @throws PublisherFailureException  if cannot connect to the kvstore
     * specified in the parameters
     */
    public static KVStore getKVStore(String storeName,
                                     String... helperHosts)
        throws PublisherFailureException {

        final KVStoreConfig kvStoreConfig = new KVStoreConfig(storeName,
                                                              helperHosts);
        try {
            return KVStoreFactory.getStore(kvStoreConfig);
        } catch (FaultException | IllegalArgumentException e) {
            final String msg = "Can not open connection to "
                               + " Oracle NoSQL store [" + storeName + "] " +
                               "at " + Arrays.toString(helperHosts) +
                               ".\nPlease make sure the store is running and " +
                               "all arguments are correct." +
                               "\nKVS config used to connect: " + kvStoreConfig;
            final boolean isRemote = (e instanceof FaultException) &&
                                     ((FaultException) e).wasLoggedRemotely();
            throw new PublisherFailureException(msg, isRemote, e);
        }
    }

    /**
     * @hidden
     *
     * Internal use in test only. Returns all live publishing units
     */
    public Map<String, PublishingUnit> getPublishingUnitMap() {
        return publishingUnitMap.entrySet().stream()
                                .filter(entry -> !entry.getValue().isClosed())
                                .collect(Collectors.toMap(Map.Entry::getKey,
                                                          Map.Entry::getValue));
    }

    /**
     * @hidden
     *
     * Adds a PU to publisher
     *
     * @param id  id of pu to add
     * @param pu  pu to add
     */
    public void addPU(String id, PublishingUnit pu) {
        publishingUnitMap.put(id, pu);
    }

    /**
     * @hidden
     *
     * Removes a pu from publisher
     *
     * @param id  id of pu to remove
     */
    public void removePU(String id) {
        publishingUnitMap.remove(id);
    }

    /**
     * @hidden
     *
     * Gets table md manager
     *
     * @return table md manager
     */
    public PublisherTableMDManager getPublisherTableMDManager() {
        return publisherTableMDManager;
    }

    /**
     * @hidden
     *
     * Gets topology manager
     *
     * @return topology manager
     */
    public PublisherTopoManager getPublisherTopoManager() {
        return publisherTopoManager;
    }

    /**
     * @hidden
     *
     * @return Gets security credentials
     */
    public SecurityCred getSecurityCred() {
        return securityCred;
    }

    /**
     * @hidden
     *
     * @return true if source kvstore is a secure store
     */
    public boolean isSecureStore() {
        return securityCred.isSecureStore();
    }

    /* Internal use in test only */
    long getWarningShardTimeoutMs() {
        return warningShardTimeoutMs;
    }

    /* Internal use in test only */
    long getMaxConcurrSubs() {
        return maxConcurrSubs;
    }

    /**
     * @hidden
     *
     * in some unit test only where table name is used as table id
     */
    public void setUseTableNameAsId() {
        tableNameAsId = true;
    }

    /**
     * @hidden
     *
     * in some unit test only where table name is used as table id
     */
    public boolean useTableNameAsId() {
        return tableNameAsId;
    }


    /**
     * @hidden
     *
     * Gets JE HA security property used by the publisher
     */
    public Properties getJEHASecurityProp() {
        return jeHASecurityProp;
    }

    /**
     * @hidden
     *
     * Gets the publisher id
     */
    public String getPublisherId() {
        return publisherId;
    }

    /**
     * @hidden
     *
     * Unit test only. In some unit tests to simulate crash and resume in
     * single JVM, we need to recreate a publisher, clear the map to avoid
     * returns old publisher
     */
    public static void clearPublisherInstMap() {
        pubInstMap.clear();
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[NoSQLPublisher-" + publisherId + "] " + msg;
    }

    private static String makePublisherId(String id, String storeName) {
        return (id != null) ? id : storeName;
    }

    /* Gets store topology */
    private Topology getStoreTopology() {
        return ((KVStoreImpl)kvs).getTopology();
    }

    /* Call user's callback onError() and handle all exceptions  */
    private void handleError(Subscriber<?> s, Throwable t) {

        try {
            s.onError(t);
        } catch (Exception exp) {
            logger.warning(lm("Exception in executing " +
                              "subscriber's onError()=" +
                              exp + "\n" +
                              LoggerUtils.getStackTrace(exp)));
        }
    }

    private boolean matchingPublisher(LoginCredentials otherLoginCred,
                                      NoSQLPublisherConfig otherConf) {

        if (!config.equals(otherConf)) {
            return false;
        }

        /* check security credentials */
        final Properties otherSecProp = otherConf.getKvStoreConfig()
                                                 .getSecurityProperties();
        final SecurityCred otherSC = buildSecurityCred(otherLoginCred,
                                                       otherSecProp);
        return securityCred.equals(otherSC);
    }

    /* Builds security credentials */
    private SecurityCred buildSecurityCred(LoginCredentials lc, Properties sp) {

        final String userName;
        final Class<?> credentialType;

        if (lc != null) {
            userName = lc.getUsername();
            credentialType = lc.getClass();
        } else if (sp != null) {

            if (sp.get(AUTH_USERNAME_PROPERTY) != null) {
                userName = sp.get(AUTH_USERNAME_PROPERTY).toString();
            } else {
                userName = null;
            }

            final String mech;
            if (sp.get(AUTH_EXT_MECH_PROPERTY) != null) {
                mech = sp.get(AUTH_EXT_MECH_PROPERTY).toString();
            } else {
                mech = "not specified";
            }

            /* TODO: Update for new mechanisms */
            if (KRB_MECH_NAME.equals(mech)) {
                credentialType = KerberosCredentials.class;
            } else {
                if (userName == null) {
                    /* non-secure store */
                    credentialType = null;
                } else {
                    /* secure store, default credential type */
                    credentialType = PasswordCredentials.class;
                }
            }
        } else {
            userName = null;
            credentialType = null;
        }

        return new SecurityCred(userName, credentialType);
    }

    /**
     * Checks if server software edition support streaming, throw PFE if not
     *
     * @throws PublisherFailureException if any shard does not support
     * streaming
     */
    private void serverEditionCheck() throws PublisherFailureException {

        final RegistryUtils regUtil =
            ((KVStoreImpl) kvs).getDispatcher().getRegUtils();
        if (regUtil == null) {
            final String msg = "The request dispatcher has not initialized " +
                               "itself yet.";
            throw new PublisherFailureException(msg, false, null);
        }
        final Topology topology = getStoreTopology();
        final Set<RepGroupId> groups = topology.getRepGroupIds();
        final PingCollector pc = new PingCollector(topology, logger);

        //TODO: it would be better to make ping collector returns version
        /*
         * Now we effectively make two pings for each shard to return
         * the version. This can be avoided if we could make the ping
         * collector utils return the version information as well.
         */

        /* Check each shard master to ensure edition is eligible to stream */
        boolean succ = false;
        for (RepGroupId gid : groups) {
            /* get master from ping */
            final RepNode master =  pc.getMaster(gid);
            if (master == null) {
                /*
                 * If the master is not found or there are more than one node
                 * that PingCollector thinks it's master, give up
                 */
                final String msg = "Shard=" + gid +
                                   " in store=" + config.getStoreName() +
                                   " has no master or more than 1 master from" +
                                   " ping, skip the check this shard for now.";
                logger.warning(lm(msg));
                continue;
            }

            try {
                final RepNodeId rid = master.getResourceId();
                /* retry to handle temp failure */
                if (publisherTopoManager.noStreamSupport(
                    regUtil, rid, MAX_RETRY_STREAM_SUPPORT)) {
                    final String msg = "Software running on id=" +
                                       master.getResourceId() +
                                       " in store=" + config.getStoreName() +
                                       " does not support streaming after max" +
                                       " retry=" + MAX_RETRY_STREAM_SUPPORT;
                    logger.warning(lm(msg));
                    throw new PublisherFailureException(msg, true, null);
                }
                succ = true;
            } catch (RemoteException re) {
                logger.fine(() -> "Unable to reach node=" +
                                  master.getResourceId() +
                                  " in store=" + config.getStoreName() +
                                  ", reason=" + re);
            } catch (NotBoundException nbe) {
                logger.fine(() -> "Unable to look up registry for=" +
                                  master.getResourceId() +
                                  " in store=" + config.getStoreName() +
                                  ", reason=" + nbe);
            } catch (InterruptedException ie) {
                final String msg = "Unexpected interruption during checking " +
                                   "server edition on=" +
                                   master.getResourceId() +
                                   " in store=" + config.getStoreName();
                logger.warning(msg + ", reason=" + ie);
                throw new PublisherFailureException(
                    msg + ", reason=" + exceptionString(ie), true, ie);
            }
        }

        if (!succ) {
            /*
             * too bad we cannot check any shard, we allow to create
             * publisher but dump some warning
             */
            final String msg = "None of the shards is accessible to check " +
                               "software edition for store=" +
                               config.getStoreName();
            logger.warning(lm(msg));
        }
    }

    /**
     * @hidden
     *
     * Security credentials used by the publisher
     */
    public static class SecurityCred {
        private final String userName;
        private final Class<?> credentialType;

        SecurityCred(String userName, Class<?> credentialType) {
            this.userName = userName;
            this.credentialType = credentialType;
        }

        public String getUserName() {
            return userName;
        }

        public Class<?> getCredentialType() {
            return credentialType;
        }

        /* Check if a secure credential for secure store */
        boolean isSecureStore() {
            return userName != null && credentialType != null;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof SecurityCred)) {
                return false;
            }
            final SecurityCred osc = (SecurityCred) object;

            /* No credentials means accessing a non-secure store */
            if ((userName == null) && (osc.getUserName() == null)) {
                return true;
            }

            /* Otherwise both sides need credentials */
            return userName != null &&
                   osc.getUserName() != null &&
                   userName.equals(osc.getUserName()) &&
                   credentialType.equals(osc.getCredentialType());
        }

        @Override
        public int hashCode() {
            return (userName == null ? 3 : userName.hashCode()) +
                (credentialType == null ? 7 : credentialType.hashCode());
        }
    }
}
