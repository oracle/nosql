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

package oracle.kv.impl.sna.masterBalance;

import java.rmi.RemoteException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Implements the MasterBalancingInterface. This class is designed to be an
 * independent service; the current implementation creates an instance of this
 * class when the SNA starts up, and is shutdown as part of the SNA's
 * shutdown. That is, its lifecycle is coincident with the SNA's lifecycle.
 * <p>
 * This instance must be created before the SNA starts up any of its RNs so
 * that none of the state changes are missed and an accurate count of the
 * masters at the SN can be maintained.
 * <p>
 * @see MasterBalancingInterface
 */
public class MasterBalanceManager implements MasterBalanceManagerInterface {

    /**
     * The storage node being balanced.
     */
    private final SNInfo snInfo;

    /**
     * The component that coordinates the use of leases during a master
     * rebalance operation.
     */
    private final MasterLeaseManager masterLeaseManager;

    /**
     * The cached copy of the topology used to identify RNs at this SN and
     * other SNs. We need this cached copy, since it's not stored persistently
     * on the SNA.
     */
    private final TopoCache topoCache;

    /**
     * The thread instance used to initiate rebalancing whenever the BMD is
     * exceeded.
     */
    private final RebalanceThread rebalanceThread;

    /**
     * Coordinates shutdown activities.
     */
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    final Logger logger;

    final LoginManager loginMgr;

    /**
     * An instance of MasterBalancing. There is one per SN. Its lifecycle
     * follows that of the SNA.
     *
     * @param snInfo information about the SN that is being balanced
     * @param logger the logger to be used
     */
    private MasterBalanceManager(SNInfo snInfo,
                                 Logger logger,
                                 LoginManager loginMgr) {

        this.snInfo = snInfo;
        this.logger = logger;
        this.loginMgr = loginMgr;

        masterLeaseManager = new MasterLeaseManager(logger);
        topoCache = new TopoCache(snInfo, logger, getLoginManager()) {
            @Override
            Set<RepNodeId> getActiveRNs() {
               return rebalanceThread.getActiveRNs();
            }
        };
        rebalanceThread = new RebalanceThread(this);
        rebalanceThread.start();
    }

    /**
     * Factory method to create a MBM
     *
     * @param enabled if true MB is enabled
     * @param snInfo information about the SN that is being balanced
     * @param logger the logger to be used
     */
    public static MasterBalanceManagerInterface create(boolean enabled,
                                                       SNInfo snInfo,
                                                       Logger logger,
                                                       LoginManager loginMgr) {
        return enabled ?
            new MasterBalanceManager(snInfo, logger, loginMgr) :
            new MasterBalanceManagerDisabled(logger);
    }

    SNInfo getSnInfo() {
        return snInfo;
    }

    RebalanceThread getRebalanceThread() {
        return rebalanceThread;
    }

    /**
     * Returns the topo cache maintained for balancing
     */
    TopoCache getTopoCache() {
        return topoCache;
    }

    /**
     * Cleanup on exit.
     */
    @Override
    public void shutdown() {
        if (shutdown.getAndSet(true)) {
            return;
        }

        topoCache.shutdown();
        masterLeaseManager.shutdown();
        rebalanceThread.shutdown();
        logger.info("Master balance manager shutdown");
    }

    /**
     * Invoked to note that a particular RN has exited. If the RN exit was not
     * abrupt, this call may have been preceded by a noteState change called
     * communicating the DETACHED state. The method is resilient in the face
     * of such redundant calls.
     */
    @Override
    public void noteExit(RepNodeId rnId) {
        if (shutdown.get()) {
            /* Ignore requests after it's been shutdown. */
            return;
        }

        masterLeaseManager.cancel(rnId);
        try {
            rebalanceThread.noteExit(rnId);
        } catch (InterruptedException e) {
            logger.info("NoteExit call interrupted");
        }
    }

    @Override
    public synchronized void noteState(StateInfo stateInfo,
                                       AuthContext authContext,
                                       final short serialVersion) {

        if (shutdown.get()) {
            /* Ignore requests after it's been shutdown. */
            return;
        }

        final RepNodeId rnId = stateInfo.rnId;
        /* Track latest current topology. */
        topoCache.noteLatestTopo(stateInfo);

        try {
            if (stateInfo.state.isMaster()) {
                /* cancel any outstanding lease for this node */
                masterLeaseManager.cancel(rnId);
            }
            rebalanceThread.noteState(stateInfo);
        } catch (InterruptedException e) {
            logger.info("State change call interrupted");
        }
    }

    @Override
    public synchronized MDInfo getMDInfo(AuthContext authContext,
                                         short serialVersion) {

        /*
         * rnCount could be 0 if the topology is out-of-date and does not
         * reflect the current state of the SN.
         */
        if (!topoCache.isInitialized() || (topoCache.getRnCount() == 0) ||
            shutdown.get()) {
            return null;
        }

        return new MDInfo(rebalanceThread.getMasterCount(),
                          masterLeaseManager.leaseCount(),
                          topoCache.getRnCount(),
                          rebalanceThread.getBMD());
    }

    @Override
    public synchronized boolean cancelMasterLease(StorageNode lesseeSN,
                                                  RepNode rn,
                                                  AuthContext authContext,
                                                  short serialVersion) {

        if (shutdown.get()) {
            return false;
        }

        return masterLeaseManager.cancel(lesseeSN, rn);
    }

    @Override
    public synchronized boolean getMasterLease(MasterLeaseInfo masterLease,
                                               AuthContext authContext,
                                               short serialVersion) {

        if (!topoCache.isInitialized() || shutdown.get()) {
            return false;
        }

        /*
         * If willShutDownServices variable is set to true, it means this SN is
         * shutting down its RNs. Don't grant the MasterLease.
         */
        if (rebalanceThread.willShutDownServices()) {
            return false;
        }

        MDInfo mdInfo = getMDInfo(null, serialVersion);
        if ((mdInfo == null) ||
            /*
             * Check if this (the target) SN's PTMD has increased since the
             * time the decision to request the lease was made by the source
             * SN. If it has, reject the lease request since that decision may
             * no longer be valid. The source SN may retry after obtaining the
             * increased PTMD.
             */
            (mdInfo.getPTMD() > masterLease.limitPTMD)) {
            return false;
        }

        /* Establish a lease, if it's in replica state. */
        return rebalanceThread.isReplica(masterLease.getRepNodeId())
            ? masterLeaseManager.getMasterLease(masterLease) : false;
    }

    void setWillRNsShutDown(boolean willRNsShutDown) {
        rebalanceThread.setShutDownServices(willRNsShutDown);
    }

    @Override
    public void transferMastersForShutdown() {
        if (!topoCache.isInitialized() || shutdown.get()) {
            return;
        }
        rebalanceThread.transferMastersForShutdown();
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   AuthContext authContext,
                                   short serialVersion)
        throws RemoteException {

        if (shutdown.get()) {
            return;
        }

        rebalanceThread.overloadedNeighbor(storageNodeId);
    }

    public LoginManager getLoginManager() {
        return loginMgr;
    }

    /**
     * Convenience "struct" used to package info about the SN.
     */
    public static class SNInfo {
        final String storename;
        final StorageNodeId snId;
        final String snHostname;
        final int snRegistryPort;

        public SNInfo(String storename,
                      StorageNodeId snId,
                      String snHostname,
                      int snRegistryPort) {
            super();
            this.storename = storename;
            this.snId = snId;
            this.snHostname = snHostname;
            this.snRegistryPort = snRegistryPort;
        }
    }
}
