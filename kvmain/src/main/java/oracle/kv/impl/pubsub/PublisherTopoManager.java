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

import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.pubsub.PublisherFailureException;

/**
 * Object that manages all topology related information of kvstore. The
 * manager is created and initialized by NoSQLPublisher and shared by all
 * subscriptions spawned by the publisher.
 */
public class PublisherTopoManager {

    private final static int CHECK_VERSION_RETRY_SLEEP_MS = 500;
    /* private logger */
    private final Logger logger;
    /* store name */
    private final String storeName;
    /* handle to store from publisher */
    private final KVStoreImpl kvs;

    /*
     * master RN id and its HA address for each shard, only master that
     * supports streaming will be put in the map
     */
    private final
    ConcurrentHashMap<RepGroupId, ShardMasterInfo> masterInfoMap;

    /**
     * Creates an instance of toplogy manager and initialize the master HA
     * hostport for each shard
     *
     * @param storeName   name of source kvstore
     * @param kvs         handle to source kvstore
     * @param logger      private logger
     *
     * @throws PublisherFailureException raised when fail to initialize the
     * master HA hostport for each shard
     */
    public PublisherTopoManager(String storeName, KVStore kvs, Logger logger)
        throws PublisherFailureException {

        this.storeName = storeName;
        this.kvs = (KVStoreImpl) kvs;
        this.logger = logger;

        /* initialize master RN HA for each shard */
        masterInfoMap = new ConcurrentHashMap<>();
        for (RepGroupId rid : getTopology().getRepGroupIds()) {
            try {
                final ShardMasterInfo info = buildMasterInfo(rid, null);
                if (info != null) {
                    masterInfoMap.put(rid, info);
                }
            } catch (FaultException fe) {
                final String msg = "Cannot create topology manager, " +
                                   "error=" + exceptionString(fe);
                throw new PublisherFailureException(msg, true, fe);
            }
        }
    }

    /**
     * Gets topology from the request dispatcher within KVS
     *
     * @return topology
     */
    public Topology getTopology() {
        return kvs.getTopology();
    }

    /**
     * Returns master RN HA address for a shard. If the master RN HA has been
     * refreshed and different from current one, returns the refreshed master
     * HA hostport. Otherwise, refresh and return the updated one.
     *
     * @param gid        id of given shard
     * @param prev       previous cached master ha hostport
     *
     * @return a refreshed master HA host port for a shard, or null if cannot
     * find new master or the new master does not support streaming.
     */
    synchronized ShardMasterInfo buildMasterInfo(RepGroupId gid,
                                                 ShardMasterInfo prev)
        throws IllegalArgumentException, FaultException {

        logger.fine(() -> lm("Try get master for shard=" + gid +
                             ", curr shard=" + prev));

        final ShardMasterInfo exist = masterInfoMap.get(gid);
        if (exist != null) {
            /* no prev master, return whatever we have */
            if (prev == null) {
                logger.fine(() -> lm("No previous master, return cached " +
                                     "master shard=" + exist));
                return exist;
            }
            /* already have a newer master, return it */
            if (exist.getTimestamp() > prev.getTimestamp()) {
                logger.fine(() -> lm("Already has a newer master for shard=" +
                                     gid + ": " + exist));
                return exist;
            }
        }

        final long start = System.currentTimeMillis();
        logger.fine(() -> lm("Need refresh master info for shard=" + gid));

        /* refresh if not exists or stale */
        final ShardMasterInfo ret = refreshMasterInfo(gid);

        if (ret == null) {
            logger.fine(() -> lm("Fail to refresh master for shard=" + gid));
            masterInfoMap.remove(gid);
            return null;
        }

        /* check if server supports streaming */
        final RegistryUtils regUtil = kvs.getDispatcher().getRegUtils();
        if (regUtil == null) {
            final String msg = "Cannot check, the request dispatcher has not" +
                               " initialized itself yet.";
            logger.fine(() -> lm(msg));
            masterInfoMap.remove(gid);
            return null;
        }

        final RepNodeId rnId = ret.getMasterRepNodeId();
        try {
            if (noStreamSupport(regUtil, rnId, 3)) {
                /* RN does not support streaming */
                final String err = "Master node=" + rnId +
                                   " does not support streaming";
                logger.warning(lm(err));
                masterInfoMap.remove(gid);
                /* caller should deal with it  */
                throw new FaultException(err, false);
            }
        } catch (RemoteException | NotBoundException | InterruptedException e) {
            /* fail to check */
            logger.fine(() -> lm("Cannot verify if node=" + rnId +
                                 " supports streaming, reason=" + e));
            masterInfoMap.remove(gid);
            return null;
        }

        /* everything looks good if reach here */
        logger.fine(() -> lm("Succeed refresh master for shard=" + gid +
                             ": " + ret + ", elapsed time(ms)=" +
                             (System.currentTimeMillis() - start)));
        masterInfoMap.put(gid, ret);
        return ret;
    }

    /**
     * Returns true if the software running on RN has no support of streaming
     *
     * @param regUtil registry util
     * @param rid     node id to check
     * @param maxTry  max # tries
     *
     * @return true if RN does not support streaming, false otherwise
     *
     * @throws RemoteException if unable to reach the RN
     * @throws NotBoundException if fail to look up in registry
     * @throws InterruptedException if interrupted
     */
    public boolean noStreamSupport(RegistryUtils regUtil,
                                   RepNodeId rid,
                                   int maxTry)
        throws RemoteException, NotBoundException, InterruptedException {

        int count = 0;
        while(true) {
            try {
                count++;
                /* Only EE server allows streaming */
                final KVVersion ver = SerialVersion.getKVVersion(
                    getSerialVersion(regUtil, rid));

                if (VersionUtil.isEnterpriseEdition(ver)) {
                    return false;
                }

                logger.fine(() -> lm("Software on rnId=" + rid +
                                     " does not support streaming.  Streaming" +
                                     " requires the Enterprise edition, but " +
                                     "found server version=" +
                                     ver.getReleaseEdition()));
                return true;
            } catch (RemoteException | NotBoundException e) {
                /*
                 * cannot start stream before we are sure the server edition
                 * supports streaming. If the shard is offline for long time,
                 * we would return false after max retries and the caller
                 * should retry.
                 */
                if (count < maxTry ) {
                    logger.fine(() -> lm("Cannot ping node=" + rid +
                                         ", will retry after time(ms)=" +
                                         CHECK_VERSION_RETRY_SLEEP_MS +
                                         " reason: " + e));
                    try {
                        synchronized (this) {
                            wait(CHECK_VERSION_RETRY_SLEEP_MS);
                        }
                    } catch (InterruptedException ie) {
                        logger.fine(() -> lm("Sleep interrupted, give up"));
                        throw ie;
                    }
                } else {
                    final int countFinal = count;
                    logger.fine(() -> lm("Cannot ping node=" + rid +
                                         " after attempts=" + countFinal +
                                         ", give up."));
                    throw e;
                }
            }
        }
    }

    /**
     * Returns the serial version used to communicate with the RepNode with the
     * given ID. The serial version is the minimum of the serial versions of
     * the caller and the server, so it can be used to determine if the server
     * has reached a particular version.
     *
     * @param regUtil registry util
     * @param rid     rep node id
     *
     * @return serial version used to communicate with given rep node
     *
     * @throws RemoteException  if unable to reach server
     * @throws NotBoundException if unable to look up the RN in registry
     */
    private short getSerialVersion(RegistryUtils regUtil, RepNodeId rid)
        throws RemoteException, NotBoundException {

        final RequestHandlerAPI api = regUtil.getRequestHandler(rid);
        if (api == null) {
            throw new NotBoundException(
                "Server was not found in the topology");
        }
        return api.getSerialVersion();
    }

    /* Refreshes shard master info */
    private ShardMasterInfo refreshMasterInfo(RepGroupId gid) {

        logger.fine(() -> lm("Try refresh master for " + gid));

        /* invalid shard id */
        if (!getTopology().getRepGroupIds().contains(gid)) {
            final String err = "Shard=" + gid + " does not exist in topology " +
                               "of store=" + storeName + " with topology id=" +
                               kvs.getTopology().getId() + ", all existing " +
                               "shards=" + Arrays.toString(getTopology()
                                                               .getRepGroupIds()
                                                               .toArray());
            throw new PublisherFailureException(err, false, null);
        }

        /* try to get it from dispatcher */
        ShardMasterInfo ret =
            ShardMasterInfo.buildFromDispatcher(kvs.getDispatcher(),
                                                gid, logger);
        if (ret != null) {
            logger.fine(() -> lm("Refreshed master from request dispatcher " +
                                 "for shard=" + gid));
            return ret;
        }

        /* try get master by ping collector */
        ret = ShardMasterInfo.buildFromPingColl(kvs, gid);
        if (ret == null) {
            logger.info(lm("Ping collector is unable locate the master " +
                           "for shard=" + gid));
        } else {
            logger.info(lm("Refreshed master from ping collector " +
                           "for shard=" + gid));
        }
        return ret;
    }

    private String lm(String msg) {
        return "[TopoMan-" + storeName + "] " + msg;
    }
}
