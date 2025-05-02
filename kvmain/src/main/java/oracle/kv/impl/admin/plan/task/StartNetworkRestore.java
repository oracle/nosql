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

package oracle.kv.impl.admin.plan.task;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Initialize a network restore from source to target node.
 */
public class StartNetworkRestore extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final ResourceId sourceId;
    private final ResourceId targetId;
    private final boolean retainOriginalLog;

    public StartNetworkRestore(AbstractPlan plan,
                               ResourceId sourceId,
                               ResourceId targetId,
                               boolean retainOriginalLog) {
        super();
        this.plan = plan;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.retainOriginalLog = retainOriginalLog;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    public static void verifyBeforeRestore(RepNodeId source,
                                           RepNodeId target,
                                           Topology topo,
                                           LoginManager loginManager,
                                           Logger logger,
                                           boolean force) {
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginManager, logger);
        final RepNodeStatus srcInfo = getRNStatus(source, regUtils, logger);
        final RepNodeStatus tarInfo = getRNStatus(target, regUtils, logger);

        if (srcInfo == null ||
            (srcInfo.getServiceStatus() != null &&
             !srcInfo.getServiceStatus().isAlive())) {
            throw new IllegalCommandException(
                "Restore source node is not alive");
        }
        if (tarInfo == null ||
            (tarInfo.getServiceStatus() != null &&
             !tarInfo.getServiceStatus().isAlive())) {
            throw new IllegalCommandException(
                "Restore target node is not alive");
        }

        /* Avoid restoring an active master from a replica */
        if (tarInfo.getIsAuthoritativeMaster()) {
            throw new IllegalCommandException("Unable to restore a master");
        }
        if (!force && srcInfo.getVlsn() <= tarInfo.getVlsn()) {
            throw new IllegalCommandException(
                "Source node doesn't have more recent data than target" +
                " node; use the force flag to perform the restore anyway");
        }

        if (force) {
            return;
        }
        int primaryRepFactor = 0;
        for (final Datacenter dc : topo.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isPrimary()) {
                primaryRepFactor += dc.getRepFactor();
            }
        }
        final int quorum = (primaryRepFactor + 1) / 2;
        int active = 0;
        for (RepNode rn : topo.getSortedRepNodes()) {
            final RepNodeId rnId = rn.getResourceId();
            if (rnId.getGroupId() == source.getGroupId() &&
                topo.getDatacenter(rnId).getDatacenterType().isPrimary()) {

                final RepNodeStatus status =
                        getRNStatus(rnId, regUtils, logger);
                if (status != null && status.getServiceStatus().isAlive()) {
                    active++;
                }
            }
        }

        /*
         * The network restore would close the target node environment, check
         * if restore would cause the replication group to lose quorum if it
         * has quorum before the restore.
         */
        if (active >= quorum && (active - 1) < quorum) {
            throw new IllegalCommandException(
                "The shard of " + target + " will lose quorum if " +
                "a network restore is performed from " + source +
                "; use the force flag to peform the restore anyway");
        }
    }

    private static RepNodeStatus getRNStatus(RepNodeId rnId,
                                             RegistryUtils regUtils,
                                             Logger logger) {
        try {
            final RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(rnId);
            return rna.ping();
        } catch (RemoteException | NotBoundException e) {
            logger.info("Unable to reach " + rnId + ": " + e);
            return null;
        }
    }

    public static void verifyIfNodesExist(Topology topology,
                                          ResourceId source,
                                          ResourceId target) {
        if (topology.get(source) == null) {
            throw new IllegalCommandException(
                "Restore source node " + source + " does not exist.");
        }
        if (topology.get(target) == null) {
            throw new IllegalCommandException(
                "Restore target node " + target + " does not exist.");
        }
    }

    @Override
    public State doWork() throws Exception {
        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        final LoginManager loginManager = admin.getLoginManager();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginManager, plan.getLogger());

        final RepNodeId srcRNId = (RepNodeId)sourceId;
        final RepNodeId tarRNId = (RepNodeId)targetId;

        /*
         * Set the latest VLSN of source node as the minimum VLSN the network
         * restore from source to target node must cover this minimum VLSN.
         */
        final RepNodeStatus rnStatus =
            getRNStatus(srcRNId, regUtils, admin.getLogger());
        final long minVLSN = rnStatus.getVlsn();
        try {
            final RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(tarRNId);
            if (!rna.startNetworkRestore(srcRNId, retainOriginalLog, minVLSN)) {
                return State.ERROR;
            }
        } catch (RemoteException | NotBoundException e) {
            throw new OperationFaultException(
                "Target restore node cannot be contacted, " + e);
        }
        return State.SUCCEEDED;
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {

        /*
         * Lock the entire replication group to make sure topology won't
         * change until the network restore is done.
         */
        final RepNodeId srcRNId = (RepNodeId)sourceId;
        LockUtils.lockRG(planner, plan, new RepGroupId(srcRNId.getGroupId()));
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
