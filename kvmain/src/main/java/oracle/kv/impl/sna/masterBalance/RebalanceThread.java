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

import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MDInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MasterLeaseInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.sna.masterBalance.ReplicaLeaseManager.ReplicaLease;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ShutdownThread;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * The thread that does the rebalancing when one is necessary. The thread
 * reacts to changes that are posted in the stateTransitions queue and
 * initiates master transfer operations in response to the state changes.
 *
 * A general overview of how the thread operate is described below.
 *
 * The thread wakes when informed about RN state changes, and checks its MD
 * against the BMD (as determined) by the method getDMD().If it finds that the
 * BMD has been exceeded it seeks to move a master from this SN to some other
 * SN in the store.
 *
 * To accomplish this it computes all the SNs hosting replicas for the current
 * set of Masters and tries to perform a master transfer to lower the MD at
 * this node. while ensuring that it does not create a master imbalance at the
 * target SN. The choice of SN can be further constrained by master affinity
 * considerations. The predicate checkRoutineTransfer() seeks to capture all
 * the constraints in a single place.
 *
 * The thread keeps trying to move masters until its MD is &lt; BMD.
 *
 * As part of Master Balancing, the method transferMastersOnShutdown is used to
 * transfer all masters away from this SN when the SN is shutdown. This is
 * expected to provide for a more graceful form of transition by minimizing any
 * resulting imbalances. A more relaxed set of rules apply during shutdown,
 * specifically we allow for transfers to SNs even if it results in the target
 * SN being imbalanced, or transfers to masterphobic nodes, if we are left with
 * no choice. The predicate checkExitTransfer implements the more relaxed
 * rules.
 */
class RebalanceThread extends ShutdownThread {
    static final String THREAD_NAME = "MasterRebalanceThread";

    /**
     * The time associated with a remote master lease. This is also the
     * amount of time, associated with the master transfer request.
     */
    private static final int MASTER_LEASE_MS = 5 * 60 * 1000;

    /**
     * The poll period used to poll for a rebalance in the absence of any
     * HA state transitions by the nodes on the SN. This permits an imbalanced
     * SN to check whether other SNs who can help ameliorate the local
     * imbalance have come online.
     */
    private static final int POLL_PERIOD_DEFAULT_MS = 1 * 60 * 1000;

    private static int pollPeriodMs = POLL_PERIOD_DEFAULT_MS;

    /**
     * The SN that is being brought into balance, by this rebalance thread.
     */
    private final SNInfo snInfo;

    /**
     * The set of replica RNs at the SN.
     */
    private final Set<RepNodeId> activeReplicas;

    /**
     * The set of master RNs at the SN. Some subset of the masters may have
     * replicaLeases associated with them, if they are in the midst of a master
     * transfer.
     */
    private final Set<RepNodeId> activeMasters;

    /**
     * The shared topo cache at the SN
     */
    private final TopoCache topoCache;

    /**
     * The leases on an RN as it transitions from master to replica as part of
     * a master transfer operation.
     */
    private final ReplicaLeaseManager replicaLeases;

    /*
     * Determines if this SN has been contacted by overcommitted SNs that
     * could potentially transfer masters to it.
     */
    private final AtomicBoolean overloadedNeighbor = new AtomicBoolean(false);

    /**
     * The queue used to post state changes to the thread that attempts to
     * correct a Master Density (MD) imbalance.
     */
    private final BlockingQueue<StateInfo> stateTransitions =
            new ArrayBlockingQueue<>(100);

    /**
     * The master balance manager associated with this thread. The
     * RebalanceThread is a component of the MasterBalanceManager.
     */
    private final MasterBalanceManager manager;

    private final Logger logger;

    private final RateLimitingLogger<String> limitingLogger;
    /*
     * Boolean variable used to indicate whether this SN will be shutting down
     * its RNs or not. Used to prevent gaining master leases from this SN when
     * the RNs in this SN will be shutting down.
     */
    private final AtomicBoolean shutdownServices = new AtomicBoolean(false);

    RebalanceThread(MasterBalanceManager manager) {
        super(THREAD_NAME);
        snInfo = manager.getSnInfo();
        topoCache = manager.getTopoCache();
        this.manager = manager;

        activeReplicas = Collections.synchronizedSet(new HashSet<>());
        activeMasters = Collections.synchronizedSet(new HashSet<>());

        this.logger = manager.logger;
        limitingLogger = new RateLimitingLogger<>(60 * 10 * 1000, 10, logger);
        replicaLeases = new ReplicaLeaseManager(logger);
    }

    @Override
    protected Logger getLogger() {
       return logger;
    }

    /**
     * Used to vary the poll period in unit tests.
     */
    static void setPollPeriodMs(int pollPeriodMs) {
        RebalanceThread.pollPeriodMs = pollPeriodMs;
    }

    /**
     * Take note of an RN that has exited by simulating a state change to
     * DETACHED
     */
    void noteExit(RepNodeId rnId)
        throws InterruptedException {
        logger.info("Rebalance thread notes " + rnId + " exited");
        noteState(new StateInfo(rnId, ReplicatedEnvironment.State.DETACHED, 0));
    }

    /**
     * Take note of the new state placing it in the stateTransitions queue.
     */
    void noteState(StateInfo stateInfo)
        throws InterruptedException {

        while (!stateTransitions.offer(stateInfo, 60, TimeUnit.SECONDS)) {
            logger.info("State transition queue is full retrying. " +
                        "Capacity:" + stateTransitions.size());
        }
        logger.info("added:" + stateInfo);
    }

    /**
     * Part of the thread shutdown protocol.
     */
    @Override
    protected int initiateSoftShutdown() {
        if (!manager.shutdown.get()) {
            throw new IllegalStateException("Expected manager to be shutdown");
        }

        /* Provoke the thread into looking at the shutdown state. */
        stateTransitions.offer(new StateInfo(null, null, 0));

        return super.initiateSoftShutdown();
    }

    /**
     * Returns true if this SN could potentially be rebalanced.
     */
    private boolean needsRebalancing(boolean overload) {

        final int rnCount = topoCache.getRnCount();

        /*
         * Fix for KVSTORE-1603. Return if either there are no masters in this
         * SN OR a stale topology is saying 0 RN's in this SN.
         */
        if (activeMasters.size() == 0 || rnCount == 0) {
            return false;
        }

        final int leaseAdjustedMD = getLeaseAdjustedMD(rnCount);
        final int BMD = getBMD();

        boolean needsRebalancing = (leaseAdjustedMD > BMD)/* Imbalance */;

        /*
         * When BMD > 0, SN is allowed to host at least a master. So, no need
         * rebalance master when SN only host a master
         */
        if (BMD > 0) {
            needsRebalancing = needsRebalancing &&
                (activeMasters.size() > 1) /* More than one master. */;
        }

        if (needsRebalancing) {

            limitingLogger.log("Needs Rebalancing " +
                               topoCache.getTopology().getSequenceNumber(),
                               Level.INFO,
                               snInfo.snId + " masters: " + activeMasters +
                               " replica leases: " + replicaLeases.leaseCount()+
                               " resident RNs: " + topoCache.getRnCount() +
                               " needs rebalancing. lease adjusted MD: " +
                               leaseAdjustedMD + " > BMD:" + BMD);
            return true;
        }

        /**
         * If one of this SNs neighbors is imbalanced, try harder and seek to
         * move a master off this SN, so that the imbalanced neighbor can in
         * turn move a master to this SN. Note that as currently implemented
         * the imbalance is only propagated to immediate neighbors as a
         * simplification. In future SNs may propagate imbalance to their
         * neighbors in turn, if they cannot move a master RN off the SN based
         * on local considerations.
         */
        final int incrementalMD = (100 / topoCache.getRnCount());
        if (overload &&
            ((leaseAdjustedMD + incrementalMD) > BMD)) {

            limitingLogger.log("Overload " +
                               topoCache.getTopology().getSequenceNumber(),
                               Level.INFO,
                               snInfo.snId + " masters: " + activeMasters +
                               " replica leases: " + replicaLeases.leaseCount()+
                               " resident RNs: " + topoCache.getRnCount() +
                               " Unbalanced neighbors initiated rebalancing. " +
                               "lease adjusted MD: " + leaseAdjustedMD +
                               " + " + incrementalMD + "> BMD:"
                               + BMD);
            return true;
        }

        return false;
    }

    /**
     * The run thread. It reacts to state changes posted to the
     * stateTransitions queue.
     */
    @Override
    public void run() {
        logger.info("Started " + this.getName());
        try {
            while (true) {

                /*
                 * Wait for RN state transitions
                 */
                final StateInfo stateInfo =
                    stateTransitions.poll(pollPeriodMs, TimeUnit.MILLISECONDS);

                /* Check if "released" for shutdown. */
                if (manager.shutdown.get()) {
                    return;
                }

                if (stateInfo != null) {
                    processStateInfo(stateInfo);
                }

                if (!topoCache.ensureTopology()) {
                    continue;
                }

                final boolean overload = overloadedNeighbor.getAndSet(false);

                if (!needsRebalancing(overload)) {
                    continue;
                }

                final List<Transfer> candidateTransfers =
                    candidateTransfers(overload);
                if (transferMaster(candidateTransfers,
                                   this::checkRoutineTransfer) != null) {
                    continue;
                }

                /* needs balancing, but no master transfer. */
                final String masters =
                    new ArrayList<>(activeMasters).toString();

                limitingLogger.log("No RNs " +
                                   topoCache.getTopology().getSequenceNumber(),
                                   Level.INFO,
                                   "No suitable RNs: " + masters +
                                   " for master transfer.");

                if ((replicaLeases.leaseCount() == 0) &&
                    (getRawMD() > getCurrentAffinityBMD())) {
                    /*
                     * Unbalanced, but no progress, inform our MCSN neighbors
                     * of this predicament in case they can help by
                     * vacating a master RN
                     */
                    informNeighborsOfOverload();
                }
            }
        } catch (InterruptedException e) {

            if (manager.shutdown.get()) {
                return;
            }

            logger.info("Lease expiration task interrupted.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, this.getName() +
                       " thread exiting due to exception.", e);
            /* Shutdown the manager. */
            manager.shutdown();
        } finally {
            replicaLeases.shutdown();
            logger.info(this.getName() + " thread exited.");
        }
    }

    private void processStateInfo(final StateInfo stateInfo) {
        /* Process the state change. */
        final RepNodeId rnId = stateInfo.rnId;

        switch (stateInfo.state) {

            case MASTER:
                activeReplicas.remove(rnId);
                activeMasters.add(rnId);
                break;

            case REPLICA:
                activeMasters.remove(rnId);
                activeReplicas.add(rnId);
                replicaLeases.cancel(rnId);
                break;

            case UNKNOWN:
            case DETACHED:
                activeMasters.remove(rnId);
                activeReplicas.remove(rnId);
                replicaLeases.cancel(rnId);
                break;
        }

        logger.info("sn: " + snInfo.snId +
                    " state transition: " + stateInfo +
                    " active masters:" + activeMasters +
                    " active replicas:" + activeReplicas.size() +
                    " replica leases:" + replicaLeases.leaseCount());
    }

    /**
     * Returns true if the node is known to be the master
     */
    @SuppressWarnings("unused")
    private boolean isMaster(RepNodeId repNodeId) {
        return activeMasters.contains(repNodeId);
    }

    /**
     * Returns true if the node is known to be a replica
     */
    boolean isReplica(RepNodeId repNodeId) {
        return activeReplicas.contains(repNodeId);
    }

    /**
     * Predicate used to winnow down candidate transfers to the subset that
     * obey the transfer invariants in two distinct situations:
     *
     *  1) In the steady state when the SN is running routinely.
     *  2) When the SN is shutting down and needs to move master off it.
     *
     */
    private interface CheckTransfer {
        /**
         * Returns true if the transfer should be performed given the
         * specified master density at the destination.
         */
        boolean checkTransfer(Transfer transfer, MDInfo targetMDInfo);
    }

    /**
     * Predicate used to validate a master transfer when the SN is running (not
     * exiting)
     */
    private boolean checkRoutineTransfer(Transfer transfer,
                                         MDInfo targetMDInfo) {
        int targetPTMD = targetMDInfo.getPTMD();
        if (targetPTMD >  100) {
            /* No room on target SN */
            return false;
        }

        boolean sourceAffinity = getMasterAffinity(snInfo.snId);
        boolean targetAffinity =
            getMasterAffinity(transfer.targetSN.getStorageNodeId());

        /* Transfers within "like affinity zones", allow if BMD permits */
        if (sourceAffinity == targetAffinity) {
            /*
             * Don't create an imbalance at the target by moving a
             * master to it.
             */
            return targetPTMD <= targetMDInfo.getBMD();
        }

        /* Differing affinities. */
        if (targetAffinity) {
            /* Source is masterphobic, move to master affinity target node even
             *  on imbalance, that is favor master affinity over imbalance.
             */
            return true;
        }

        /**
         * Target is masterphobic, but source isn't. Never move away from a
         * master affinity SN, except when exiting the SN and we have exhausted
         * all other choices.
         */
        return false;
    }

    /**
     * Predicate used to validate a master transfer when the SN exits and needs
     * to transfer masters away from it.
     */
    private boolean checkExitTransfer
        (@SuppressWarnings("unused") Transfer transfer,
         MDInfo targetMDInfo) {
        final int targetPTMD = targetMDInfo.getPTMD();
        if (targetPTMD >  100) {
            return false;
        }
        /* Always allow. a transfer if there is room at the node. */
        return true;
    }

    /**
     * Request a single master transfer from the ordered list of transfer
     * candidates, or null if none is found.
     */
    private Transfer transferMaster(List<Transfer> candidateTransfers,
                                    CheckTransfer checkTransfer) {

        if (candidateTransfers.isEmpty()) {
            return null;
        }

        final Topology topo = topoCache.getTopology();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, manager.getLoginManager(), logger);
        final StorageNode localSN = topo.get(snInfo.snId);

        for (Transfer transfer : candidateTransfers) {

            /* Contact the local RN and request the transfer. */
            logger.info("Requesting master RN transfer : " + transfer);

            StorageNodeAgentAPI sna = null;
            RepNodeId sourceRNId = null;
            StorageNodeId targetSNId =
                transfer.targetSN.getResourceId();

            boolean masterTransferInitiated = false;
            try {
                sna = regUtils.getStorageNodeAgent (targetSNId);
                MDInfo targetMDInfo = sna.getMDInfo();
                if (targetMDInfo == null) {
                    logger.info("Rejecting transfer: " + transfer +
                                " target SN:" + transfer.targetSN +
                                " MD info no longer available");
                    continue;
                }

                if (!checkTransfer.checkTransfer(transfer, targetMDInfo)) {
                    logger.info("Rejecting transfer: " + transfer + " " +
                        targetMDInfo +
                        " check:" + checkTransfer);
                    continue;
                }

                final MasterLeaseInfo masterLease =
                    new MasterLeaseInfo(localSN,
                                        transfer.targetRN,
                                        transfer.ptmd,
                                        MASTER_LEASE_MS);
                if (!sna.getMasterLease(masterLease)) {
                    continue;
                }

                sourceRNId = transfer.sourceRN.getResourceId();
                final ReplicaLease replicaLease = new ReplicaLeaseManager.
                    ReplicaLease(sourceRNId, MASTER_LEASE_MS);

                replicaLeases.getReplicaLease(replicaLease);

                RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(sourceRNId);
                masterTransferInitiated =
                    rna.initiateMasterTransfer(transfer.targetRN.getResourceId(),
                                               MASTER_LEASE_MS,
                                               TimeUnit.MILLISECONDS);
                if (masterTransferInitiated) {
                    return transfer;
                }

            } catch (RemoteException | NotBoundException e) {
               continue;
            } catch (InternalFaultException re) {
                /* An internal fault in a remote request, log and proceed. */
                logger.log(Level.WARNING,
                           "Unexpected fault during transfer:" + transfer, re);
                continue;
            } finally {
                if (!masterTransferInitiated) {
                    /* Clean up leases. */
                    if (sna != null) {
                        try {
                            sna.cancelMasterLease(localSN, transfer.targetRN);
                        } catch (ConnectException e) {
                            /* Network exceptions are routine */
                            logger.fine("Could not contact SN for master " +
                                        "lease cleanup: " + targetSNId +
                                        " reason:" + e.getMessage());
                        } catch (RemoteException e) {
                            logger.info("Failed master lease cleanup: " +
                                        e.getMessage());
                        } catch (InternalFaultException re) {
                            /*
                             * An internal fault in a remote request, log and
                             * proceed.
                             */
                            logger.log(Level.WARNING,
                                       "Failed master lease cleanup:" +
                                        transfer, re);
                            continue;
                        }
                    }
                    if (sourceRNId != null) {
                        replicaLeases.cancel(sourceRNId);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Selects suitable target SNs for the master transfer, if doing so
     * will reduce the MD at this SN and bring it under the BMD.
     *
     * It contacts all the SNs in Master Candidate SNs (MCSNs) to get a
     * snapshot of their current post-transfer MDs (the MD after an additional
     * master has been transferred to it). If this candidate SN has a lower
     * post-transfer MD than the post-transfer MD (the MD with one less master)
     * of the overloaded SN, the SN is selected as a potential target for the
     * master transfer. In the case of a tie, the selection process may use
     * other criteria, like switch load balancing, as a tie breaker.
     *
     * The transfers are returned as a list ordered by affinity (master
     * affinity first), and within affinity by increasing Post-Transfer Master
     * Density (PTMD). If this node is master phobic, transfers to master
     * affinity SNs is prioritized, over transfers to master phobic SNs, even
     * if doing so will result in an overload at the target RN. However, if
     * this is a master affinity node, transfers are only allowed to other
     * master affinity nodes.
     *
     * Note that if a neighbor SN is overloaded, then the MD density associated
     * with this SN is boosted by one in order to consider a master transfer in
     * situations where it may not have otherwise.
     *
     * @param overload used to indicate whether a neighbor SN is overloaded
     *
     * @return an ordered list of potential transfers.
     */
    private List<Transfer> candidateTransfers(boolean overload) {

        final Topology topology = topoCache.getTopology();
        final Set<StorageNode> mcsns = getMCSNs();

        limitingLogger.log("MCSNS " + topology.getSequenceNumber(),
                           Level.INFO, "MCSNS:" + mcsns);
        final TreeMap<Boolean, TreeMap<Integer, List<StorageNode>>>
            znAffinities = orderMCSNs(mcsns);
        final List<Transfer> transfers = new LinkedList<>();

        /* Select an RN to move */
        final int loweredMD =
            ((activeMasters.size() +
              (overload ? 1 : 0) -
              replicaLeases.leaseCount() - 1) * 100) /
            topoCache.getRnCount();

        final boolean sourceAffinity = getMasterAffinity(snInfo.snId);
        for (Entry<Boolean, TreeMap<Integer, List<StorageNode>>>
            znAffinityEntry : znAffinities.entrySet()) {

            final boolean targetAffinity = znAffinityEntry.getKey();

            if ((sourceAffinity == targetAffinity) && (loweredMD <= 0)) {
                /*
                 * No further candidates when transferring to SNs with the
                 * same master affinity and the transfer would move all
                 * masters from this SN.  Note that we do allow such
                 * transfers when moving SNs from no master affinity to
                 * master affinity zones.
                 */
                return transfers;
            }

            for (final Entry<Integer, List<StorageNode>> entry :
                    znAffinityEntry.getValue().entrySet()) {

                final int ptmd = entry.getKey();
                limitingLogger.log("Candidate " +
                                    topology.getSequenceNumber() +
                                   " " + ptmd,
                                   Level.INFO,
                                   "Current affinity candidate " +
                                   " ptmd:" + ptmd +
                                   " SNS:" + entry.getValue());
                findSNCandidateTransfers(entry.getValue(), transfers, ptmd);
            }
        }
        return transfers;
    }

    /**
     * Find the transfer candidates in the list of target SNs.
     */
    private void findSNCandidateTransfers(List<StorageNode> targetSNs,
                                          List<Transfer> transfers,
                                          int ptmd) {
        final Topology topology = topoCache.getTopology();

        for (final StorageNode targetSN : targetSNs) {
            final StorageNodeId targetSnId = targetSN.getResourceId();
            /* Pick a master RN to transfer. */
            for (RepNodeId mRnId : activeMasters) {
                if (replicaLeases.hasLease(mRnId)) {
                    /* A MT for this RN is already in progress. */
                    continue;
                }
                final RepNode sourceRN = topology.get(mRnId);

                if (sourceRN == null) {
                    /*
                     * The SNA heard about the master state transition before
                     * it had a chance to update its cached topology.
                     */
                    continue;
                }
                /* Pick the target for the source. */
                final Collection<RepNode> groupNodes =
                    topology.get(sourceRN.getRepGroupId()).getRepNodes();
                for (RepNode targetRN : groupNodes) {
                    if (targetRN.getStorageNodeId().equals(targetSnId)) {
                        final Transfer transfer =
                            new Transfer(sourceRN, targetRN, targetSN, ptmd);
                        transfers.add(transfer);
                    }
                }
            }
        }
    }

    /**
     * A comparator that sorts Boolean values in the opposite order, with True
     * first.
     */
    private class TrueFirstComparator implements Comparator<Boolean> {
        @Override
        public int compare(Boolean o1, Boolean o2) {
            return o2.compareTo(o1);
        }
    }

    /**
     * Returns the MCSNs ordered by their current zone master affinity/PTMDs.
     * The main map is indexed by zone master affinity, with master affinity
     * sorted first, and no affinity second.  The map stored for each affinity
     * is indexed by PTMD with lower values first.  As a result, iterating
     * through the nested maps will visit the most desirable candidates first.
     * SNs that cannot be contacted are eliminated from this computation.
     *
     * SNs with a PTMD > 100 are eliminated, since all RNs are already
     * masters at that target.
     */
    private TreeMap<Boolean, TreeMap<Integer, List<StorageNode>>>
        orderMCSNs(Set<StorageNode> mcsns) {

        final TreeMap<Boolean, TreeMap<Integer, List<StorageNode>>>
            znAffinities = new TreeMap<>(new TrueFirstComparator());
        for (StorageNode sn : mcsns) {
            try {
                final StorageNodeAgentAPI sna =
                    RegistryUtils.getStorageNodeAgent(
                        snInfo.storename, sn, manager.getLoginManager(),
                        logger);
                MDInfo mdInfo = sna.getMDInfo();
                if (mdInfo == null) {
                    continue;
                }

                final int ptmd = mdInfo.getPTMD();
                if (ptmd > 100) {
                    /* Note that we cannot prune out ptmd > bmd transfers yet
                     * since we allow MDs to exceed BMD when there are master
                     * affinity zones, or during SN shutdown.
                     */
                    continue;
                }

                final boolean targetAffinity =
                    getMasterAffinity(sn.getStorageNodeId());
                TreeMap<Integer, List<StorageNode>> ptmds =
                    znAffinities.computeIfAbsent(targetAffinity,
                                                 (k) -> new TreeMap<>());
                /* Accumulate SN */
                ptmds.computeIfAbsent(ptmd, (k)-> new LinkedList<>()).add(sn);
            } catch (ConnectException e) {
                /* Network exceptions are routine */
                logger.fine("Could not contact SN to compute PTMD: " + sn +
                           " reason:" + e.getMessage());
                continue;
            } catch (RemoteException e) {
                logger.info("Could not get MDInfo from: " + sn +
                            " reason:" + e.getMessage());
                continue;
            } catch (NotBoundException e) {
                /* Should not happen. */
                logger.warning(sn + " missing from registry.");
                continue;
            } catch (InternalFaultException re) {
                /* An internal fault in a remote request, log and proceed. */
                logger.log(Level.WARNING,
                           "Unexpected fault at remote SN:" + sn, re);
                continue;
            }
        }
        return znAffinities;
    }

    /**
     * Returns the set of Master Candidate SNs (MCSNs) that are candidates for
     * a MasterTransfer. That is, they host replica RNs that could trade state
     * with master RNs at this SN. The result set excludes SN from secondary
     * data centers.
     */
    private Set<StorageNode> getMCSNs() {
        final Set<StorageNode> mcsns = new HashSet<>();

        final Topology topology = topoCache.getTopology();

        for (RepNodeId mRnId : activeMasters) {

            final RepNode mrn = topology.get(mRnId);

            if (mrn == null) {
                /*
                 * A topology race, have heard of master but don't yet have
                 * an updated topology.
                 */
                continue;
            }
            final RepGroupId mRgId = mrn.getRepGroupId();

            if (replicaLeases.hasLease(mRnId)) {
                /* Already a master transfer candidate. */
                continue;
            }

            mapRNsToSNs(topology, topology.get(mRgId).getRepNodes(), mcsns);
        }
        return mcsns;
    }

    /**
     * Returns the raw (unadjusted for any in-flight master transfers) Master
     * Density (MD).
     */
    private int getRawMD() {
        return (!topoCache.isInitialized() || manager.shutdown.get()) ?
            Integer.MAX_VALUE :
            ((activeMasters.size() * 100) / topoCache.getRnCount());
    }

    /**
     * Subtract replica leases to account for master transfers that are in
     * play.
     */
    private int getLeaseAdjustedMD(int rnCount) {
        return ((activeMasters.size() - replicaLeases.leaseCount()) * 100) /
            rnCount;
    }

    /**
     * Returns the Balanced Master Density (BMD) for nodes on this SN, as a
     * percentage of the RNs on this SN that can simultaneously be masters on
     * this SN. If the master density at this SN exceeds its BMD, this SN is
     * considered to be out of balance and an attempt will be made to move
     * masters to a different SN until MD is once again &lte; BMD.
     *
     * If there are zones with master affinity, then the value only takes into
     * account nodes that are in master affinity zones.
     */
    int getBMD() {
        Topology topo = topoCache.getTopology();
        boolean hasMasterAffinity = false;

        /* Find the master affinity in topology */
        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            if (dc.getMasterAffinity()) {
                hasMasterAffinity = true;
                break;
            }
        }

        if (!hasMasterAffinity) {
            return rfToBMD(topoCache.getPrimaryRF());
        }

        /*
         * Should be no masters if this SN's zone does not have master affinity
         * and other zones do.
         */
        if (!topo.getDatacenter(snInfo.snId).getMasterAffinity()) {
            return 0;
        }

        /* Find information about zones with master affinity */
        int totalRFAffinityZone = 0;
        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            if (dc.getMasterAffinity()) {
                totalRFAffinityZone += dc.getRepFactor();
            }
        }
        return rfToBMD(totalRFAffinityZone);
    }

    /**
     * Convert RF to BMD; it's effectively 1/RF rounded up
     *
     * For SNs where the capacity is not a multiple of RF, the BMD is rounded
     * up to a multiple to allow for more masters at the SN. This rounding up
     * prevents MB from repeatedly trying to balance SNs where the capacity is
     * not a multiple of RF, e.g. a 16X3 on 3 SNs, which calls for 16 masters
     * distributed across 3 machines. For SNs where the capacity is <= RF, the
     * BMD is 100/rnCount, that is, one master
     *
     * @param rf the replication factor
     *
     * @return the BMD
     */
    private int rfToBMD(int rf) {

        /*
         * The actual number of RNs on the SN. It is like the capacity, but
         * represents the part of the capacity that is actually needed for the
         * topology. For example, if there were three SNs, two with capacity
         * 16, and one with capacity 17, the one with capacity 17 would have
         * rnCount=16, because the additional capacity on that SN is not used.
         */
        final int rnCount = topoCache.getRnCount();

        /*
         * The maximum number of masters that represents a balanced number for
         * this SN, rounding up to account for the fact that the best possible
         * result may need to include a rounding error.
         */
        if (rnCount <= rf) {
            /* One master */
            return 100 / rnCount;
        }

        /* rnCount > rf */
        final int balancedMasters = (rnCount + rf - 1) / rf; /* Rounded up. */
        return (balancedMasters * 100 ) / rnCount ;
    }

    /**
     * Returns the balanced master density when masters should be transferred
     * to the zones which has the same master affinity as the zone hosting the
     * current SN.
     */
    int getCurrentAffinityBMD() {
        Topology topo = topoCache.getTopology();
        boolean targetMasterAffinity =
                topo.getDatacenter(snInfo.snId).getMasterAffinity();

        int totalRFAffinityZone = 0;
        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            boolean masterAffinity = dc.getMasterAffinity();
            if ((masterAffinity == targetMasterAffinity) &&
                (dc.getDatacenterType().isPrimary())) {
                totalRFAffinityZone += dc.getRepFactor();
            }
        }

        /*
         * Calculate the BMD when all masters should be transferred to the
         * zones whose affinity is equal as the current one
         */

        int currentAffinityBMD =  100 / totalRFAffinityZone;
        return currentAffinityBMD;
    }

    /**
     * Returns a count of the current masters.
     */
    int getMasterCount() {
        return activeMasters.size();
    }

    /**
     * Returns a count of the current replicas.
     */
    int getReplicaCount() {
        return activeReplicas.size();
    }

    /**
     * Returns the masters and replica that are active at the SN
     */
    Set<RepNodeId> getActiveRNs() {
        final Set<RepNodeId> activeRNs = new HashSet<>();
        activeRNs.addAll(activeMasters);
        activeRNs.addAll(activeReplicas);

        return activeRNs;
    }

    /**
     * Contact all MCSNs exhorting them to try harder to shed a master,
     * permitting this SN in turn to move a master over.
     */
    private void informNeighborsOfOverload() {

        for (StorageNode sn : getMCSNs()) {
            try {
                /* Should we request a push to just one SN and then exit? */
                final StorageNodeAgentAPI sna =
                    RegistryUtils.getStorageNodeAgent(
                        snInfo.storename, sn, manager.getLoginManager(),
                        logger);
                sna.overloadedNeighbor(snInfo.snId);
                limitingLogger.log("Push " +
                                   topoCache.getTopology().getSequenceNumber() +
                                   " " + sn.getStorageNodeId(),
                                   Level.INFO,
                                   "master rebalance push requested: " + sn);
            } catch (ConnectException e) {
                /* Network exceptions are routine */
                logger.fine("Could not contact SN for rebalance push: " + sn +
                           " reason:" + e.getMessage());
                continue;
            } catch (RemoteException e) {
                logger.info("Could not contact SN to request master " +
                            "rebalance push: " + sn + " reason:" +
                            e.getMessage());
                continue;
            } catch (NotBoundException e) {
                /* Should almost never happen. */
                logger.warning(sn + " missing from registry.");
                continue;
            } catch (InternalFaultException re) {
                /* An internal fault in a remote request, log and proceed. */
                logger.log(Level.WARNING,
                           "Unexpected fault at remote SN:" + sn, re);
                continue;
            }
        }
    }

    /**
     * Note unbalanced neighbor SN
     */
    void overloadedNeighbor(StorageNodeId storageNodeId) {
        limitingLogger.log("Neighbor " +
                           storageNodeId, Level.INFO,
                           "Master unbalanced neighbor sn:" +
                           storageNodeId);
        overloadedNeighbor.set(true);
    }

    public void setShutDownServices(boolean shutdownServices) {
        this.shutdownServices.set(shutdownServices);
    }

    public boolean willShutDownServices() {
        return shutdownServices.get();
    }

    /**
     * Initiate master transfers for RNs that have masters on the current SN in
     * preparation for shutting down the SN. This method also causes the master
     * balance manager to reject requests to transfer masters to this SNA.
     */
    public void transferMastersForShutdown() {
        logger.info("Initiating master transfers for RNs " +
                    "in preparation for shutting down the SN");

        if (!topoCache.ensureTopology()) {
            return;
        }

        /*
         * Mark the boolean flag true indicating that this SN will be shutting
         * down its RNs. On noting this flag, other SNs will not be able to
         * lease the master in this SN.
         */
        if (!shutdownServices.compareAndSet(false, true)) {
            return;
        }

        Topology topology = topoCache.getTopology();
        Set<RepNodeId> activeMasterSet = new HashSet<>(activeMasters);
        final List<RepNodeId> allMasterTransferSourceRNs = new LinkedList<>();
        /*
         * Perform transfers one at a time so that each transfer can take into
         * account the results of the previous one, to avoid overloading the
         * target SNs.
         */
        for (RepNodeId rnId : activeMasterSet) {

            final int remainingMasters =
                activeMasters.size() - replicaLeases.leaseCount();

            if (remainingMasters <= 0) {
                /*
                 * No more masters or all masters already master transfer
                 * candidates.
                 */
                break;
            }

            RepNode masterRN = topology.get(rnId);
            if (masterRN == null) {
                /*
                 * Topology race condition. The node has been heard of but
                 * doesn't have an updated topology yet.
                 */
                continue;
            }

            if (replicaLeases.hasLease(rnId)) {
                /*
                 * Already a master transfer candidate.
                 */
                continue;
            }
            final List<Transfer> transfers = new LinkedList<>();
            Collection<RepNode> groupNodes = topology.
                get(masterRN.getRepGroupId()).getRepNodes();
            Set<StorageNode> mcsns = mapRNsToSNs(topology, groupNodes);
            if (mcsns.isEmpty()) {
                continue;
            }
            /*
             * Calculate the PTMD for all the neighbors after each Master
             * transfer to take into account the results of the previous
             * one.
             */
            TreeMap<Boolean, TreeMap<Integer, List<StorageNode>>>
            znAffinities = orderMCSNs(mcsns);
            for (final Entry<Boolean, TreeMap<Integer, List<StorageNode>>>
                znAffinityEntry : znAffinities.entrySet()) {
                final TreeMap<Integer, List<StorageNode>> ptmds =
                    znAffinityEntry.getValue();

                for (final Entry<Integer, List<StorageNode>> entry :
                    ptmds.entrySet()) {
                    final int ptmd = entry.getKey();
                    for (final StorageNode targetSN : entry.getValue()) {
                        final StorageNodeId targetSNId =
                            targetSN.getStorageNodeId();
                        for (RepNode targetRN : groupNodes) {
                            if (targetRN.getStorageNodeId().
                                equals(targetSNId)) {
                                Transfer transfer =
                                    new Transfer(masterRN, targetRN,
                                                 targetSN, ptmd);
                                transfers.add(transfer);
                            }
                        }
                    }
                }
                /*
                 * All the candidate transfers listed in increasing order
                 * of PTMD so that the transfer is to the one with the
                 * least PTMD.
                 */
                if (!transfers.isEmpty()) {
                    Transfer masterTransfer =
                        transferMaster(transfers, this::checkExitTransfer);
                    if (masterTransfer != null) {
                        allMasterTransferSourceRNs
                        .add(masterTransfer.sourceRN.getResourceId());
                        logger.info("Master transfer of RN: " +
                            masterRN.getResourceId() +
                            " initiated");
                    } else {
                        logger.info("Master transfer of RN: " +
                            masterRN.getResourceId() +
                            " was not initiated");
                    }
                }
            }
        }
        /*
         * Time interval for all the initiated master transfers to complete.
         * We use the same time interval to wait for all the RepNodeServices
         * to shutdown.
         */
        final int transferWaitMillis = 120000;
        waitForRNTransfer(allMasterTransferSourceRNs, transferWaitMillis);
    }

    /**
     * Map the collection of RNs to the primary SNs which host them and add
     * them to the set of sns
     */
    private Set<StorageNode> mapRNsToSNs(Topology topology,
                                         Collection<RepNode> rns,
                                         Set<StorageNode> sns) {
        for (RepNode rn : rns) {

            StorageNodeId targetSNId = rn.getStorageNodeId();
            if (targetSNId.equals(snInfo.snId)) {
                continue;
            }
            StorageNode targetSN = topology.get(targetSNId);
            if (!topology.get(targetSN.getDatacenterId())
                .getDatacenterType().isPrimary()) {
                /*
                 * Non PRIMARY datacenter's can't host master RNs
                 */
                continue;
            }
            sns.add(targetSN);
        }
        return sns;
    }

    private Set<StorageNode> mapRNsToSNs(Topology topology,
                                         Collection<RepNode> rns) {
        return mapRNsToSNs(topology, rns, new HashSet<StorageNode>());
    }

    /*
     * Wait for all the initiated master transfers to complete.
     */
    private void waitForRNTransfer(List<RepNodeId> allMasterTransferSourceRNs,
        int transferWaitMillis) {
        final List<RepNodeId> currentMasterTransferSourceRNs =
            new LinkedList<>();

        final List<RepNodeId> nextMasterTransferSourceRNs =
            new LinkedList<>();
        nextMasterTransferSourceRNs.addAll(allMasterTransferSourceRNs);

        new PollCondition(1000, transferWaitMillis) {
            @Override
            protected boolean condition() {
                currentMasterTransferSourceRNs.clear();
                currentMasterTransferSourceRNs.
                    addAll(nextMasterTransferSourceRNs);
                for (RepNodeId sourceRNId :
                    currentMasterTransferSourceRNs) {
                    /*
                     * If replicaLease does not have the sourceRNId it means
                     * that the master transfer has been successfully
                     * completed.
                     */
                    if (!replicaLeases.hasLease(sourceRNId)) {
                        nextMasterTransferSourceRNs.remove(sourceRNId);
                    }
                }
                if (nextMasterTransferSourceRNs.isEmpty()) {
                    return true;
                }
                return false;
            }
        }.await();

        for (RepNodeId rnId : allMasterTransferSourceRNs) {
            if (nextMasterTransferSourceRNs.contains(rnId)) {
                logger.info("Master transfer of RepNode: " + rnId +
                    " timed out");
            } else {
                logger.info("Master transfer of RepNode: " + rnId +
                    " completed");
            }
        }
    }

    /**
     * Get the master affinity associated with the target SN
     */
    private boolean getMasterAffinity(StorageNodeId targetSNId) {
        return topoCache.getTopology().getDatacenter(targetSNId).
            getMasterAffinity();
    }

    /**
     * The struct containing the RNs and the target SN involved in a master
     * transfer.
     */
    private static final class Transfer {
        final RepNode sourceRN;
        final RepNode targetRN;
        final StorageNode targetSN;
        final private int ptmd;

        Transfer(RepNode sourceRN, RepNode targetRN,
                 StorageNode targetSN, int ptmd) {
            super();
            this.sourceRN = sourceRN;
            this.targetRN = targetRN;
            this.targetSN = targetSN;
            this.ptmd = ptmd;
        }

        @Override
        public String toString() {
            return "<Transfer master from " + sourceRN + " to " +
                   targetRN + " at " + targetSN + " ptmd: " + ptmd + ">";
        }
    }
}
