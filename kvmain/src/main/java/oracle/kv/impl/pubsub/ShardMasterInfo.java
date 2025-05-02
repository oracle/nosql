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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.rep.admin.ClientRepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.PingCollector;

/**
 * Object represents the mast HA hostport of a given shard and utility
 * functions to obtain it from kvstore.
 */
public class ShardMasterInfo {

    /* # of retry when trying refresh master HA hostport */
    private final static int MAX_NUM_RETRY = 3;
    /* sleep time in ms before retry  */
    private final static int SLEEP_MS_BEFORE_RETRY = 100;

    /* shard id */
    private final RepGroupId shardId;
    /* master of the shard */
    private final RepNodeId masterId;
    /* HA host and port of the master */
    private final String masterHostPort;
    /* timestamp the info is refreshed */
    private final long timestamp;

    public ShardMasterInfo(RepGroupId shardId,
                           RepNodeId masterId,
                           String masterHostPort,
                           long timestamp) {

        this.shardId = shardId;
        this.masterId = masterId;
        this.masterHostPort = masterHostPort;
        this.timestamp = timestamp;
    }


    RepGroupId getShardId() {
        return shardId;
    }

    RepNodeId getMasterRepNodeId() {
        return masterId;
    }

    String getMasterHostPort() {
        return masterHostPort;
    }

    long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "master (shard=" + shardId + ", " +
               "master RN=" + masterId + ", " +
               "haHostport=" + masterHostPort + ", " +
               "timestamp=" + FormatUtils.formatDateTimeMillis(timestamp) + ")";
    }

    /**
     * Builds master info from request dispatcher
     *
     * @param dispatcher     request dispatcher
     * @param gid            shard id
     * @param logger         logger
     *
     * @return a master info or null if fail to master info
     */
    static ShardMasterInfo buildFromDispatcher(RequestDispatcher dispatcher,
                                               RepGroupId gid,
                                               Logger logger) {

        final int sleepMs = 2 * ((RequestDispatcherImpl)dispatcher)
            .getStateUpdateIntervalMs();

        int attempts = 0;
        while(attempts < MAX_NUM_RETRY) {

            /* ask dispatcher for master */
            final RepNodeState master = dispatcher.getRepGroupStateTable()
                                                  .getGroupState(gid)
                                                  .getMaster();
            try {
                return getMasterInternal(dispatcher, master, gid);
            } catch (Exception cause) {
                logger.fine(() -> lm("Cannot ping master rn=" +
                                     master.getRepNodeId() +
                                     "reason=" + cause, gid));
            }

            attempts++;
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                final String err = "interrupted when waiting for" +
                                   " dispatcher to refresh the topology";
                logger.fine(() -> lm(err, gid));
                break;
            }
        }

        return null;
    }

    /**
     * Builds master info via ping collector facility, less efficient than
     * dispatcher. It shall be used only after dispatcher mechanism is
     * not sufficient.
     *
     * @param kvs  kvstore handle
     * @param gid  shard id
     *
     * @return master info for given shard
     */
    static ShardMasterInfo buildFromPingColl(KVStoreImpl kvs, RepGroupId gid) {

        final PingCollector pc =
            new PingCollector(kvs.getTopology(), kvs.getLogger());
        final RepNode rn =  pc.getMaster(gid);
        if (rn == null) {
            /*
             * If the master is not found or there are more than one node
             * that PingCollector thinks it's master
             */
            return null;
        }

        final RepNodeId masterRNId = rn.getResourceId();
        final PingCollector.RNNameHAPort ha = pc.getMasterNamePort(gid);
        final String haHostPort = ha.getHAHostPort();
        return new ShardMasterInfo(gid, masterRNId, haHostPort,
                                   System.currentTimeMillis());
    }

    /* build master info from master node */
    private static ShardMasterInfo getMasterInternal(RequestDispatcher
                                                         dispatcher,
                                                     RepNodeState master,
                                                     RepGroupId gid)
        throws Exception {

        if (master == null || master.getRepState() == null ||
            !master.getRepState().isMaster()) {
            return null;
        }

        final RepNodeId masterRNId = master.getRepNodeId();

        /* ping master to get HA host and port */

        final String hostport = pingMasterWithRetry(dispatcher.getRegUtils(),
                                                    masterRNId);

        /* good, ping successfully */
        return new ShardMasterInfo(gid, masterRNId, hostport,
                                   System.currentTimeMillis());
    }

    /* Gets the master RN HA address via ping */
    private static String pingMasterWithRetry(RegistryUtils regUtils,
                                              RepNodeId rnId)
        throws Exception {

        /*
         * The publisher running on client JVM and has no access to RN
         * parameters via RepNodeParams.getJENodeHostPort. Therefore we
         * have to make a remote call to the RN admin to get the JE port
         */
        int count = MAX_NUM_RETRY;
        Exception cause;
        do {
            try {
                final ClientRepNodeAdminAPI rna =
                    regUtils.getRepNodeAdmin(rnId);
                return rna.getHAHostPort();
            } catch(RemoteException | NotBoundException exp){
                count--;
                cause = exp;
                Thread.sleep(SLEEP_MS_BEFORE_RETRY);
            }
        } while(count >= 0);

        throw cause;
    }

    private static String lm(String msg, RepGroupId gid) {
        return "[MasterInfo-" + gid + "] " + msg;
    }
}
