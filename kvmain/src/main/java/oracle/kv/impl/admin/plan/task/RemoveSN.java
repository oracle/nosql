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

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Update parameters to disable all the services running on the target storage
 * node. This is purely a change to the admin database.We assume that the
 * target node is already down, and that there is no need to issue remote
 * requests to stop those services.
 *
 * Suppose we are migrating the services on {@literal SN1 -> SN20}, which
 * causes the topology to change from version {@literal 5 -> 6}, and suppose
 * that succeeds. The basic steps that occurred were:
 *
 *  1. create a new topo and params
 *  2. broadcast the topo changes
 *  3. ask the new SN to create the desired services.
 *
 * If we repeat this plan, the second plan execution will find that there are
 * no topology changes between what is desired and what is currently stored in
 * the admin db. We placidly accept this and continue nevertheless to do steps
 * 2 and 3, because we do not know whether the previous attempt was interrupted
 * between steps 1 and 2, or whether it succeeded. Because of that, if there
 * are no changes to the topology found, we wil broadcast the entire topology
 * instead of just the delta.
 */
public class RemoveSN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final StorageNodeId target;
    private final TopologyPlan plan;

    public RemoveSN(TopologyPlan plan,
                    StorageNodeId target) {

        super();
        this.plan = plan;
        this.target = target;
    }

    @Override
    protected TopologyPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        /*
         * By the time we get here, we have verified that there are no services
         * hosted on this SN, and that it has been stopped and does not respond
         * to a ping. Remove this SN from any owning storage node pools, the
         * Admin params, and the topology.
         */

        /* Remove it from the storage node pools */
        final Topology currentTopo = plan.getTopology();
        currentTopo.remove(target);

        /*
         * Save topo and params to the administrative db to preserve a
         * consistent view of the change. Note that if this plan has been
         * retried it's possible that the topology created by this plan
         * has already been saved.
         */
        if (plan.isFirstExecutionAttempt()) {
            plan.getAdmin().saveTopoAndRemoveSN(currentTopo,
                                                plan.getDeployedInfo(),
                                                target, plan);
        }

        /* Send topology changes to all nodes.*/
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             currentTopo,
                                             getName(),
                                             plan.getAdmin().getParams().
                                                            getAdminParams(),
                                             plan)) {
            return Task.State.INTERRUPTED;
        }

        final AdminServiceParams params = plan.getAdmin().getParams();
        final SecurityParams securityParams = params.getSecurityParams();
        final SecurityMetadata md = plan.getAdmin().getMetadata(
            SecurityMetadata.class, MetadataType.SECURITY);

        if (securityParams.isSecure() && md != null) {
            final String[] authMethods =
                params.getGlobalParams().getUserExternalAuthMethods();
            if (SecurityUtils.hasKerberos(authMethods) &&
                !cleanupKrbPrincipal(md, currentTopo)) {

                return Task.State.INTERRUPTED;
            }
        }

        return Task.State.SUCCEEDED;
    }

    /**
     * Remove the service principal name of this storage node from security
     * metadata and broadcast the updated security metadata to RNs.
     */
    private boolean cleanupKrbPrincipal(SecurityMetadata md,
                                        Topology currentTopo) {
        if (md.removeKrbInstanceName(target) != null) {
            plan.getAdmin().saveMetadata(md, plan);

            if (!Utils.broadcastMetadataChangesToRNs(plan.getLogger(),
                                                     md,
                                                     currentTopo,
                                                     "remove SN Kerberos" +
                                                     " principal " + target,
                                                     plan.getAdmin().
                                                         getParams().
                                                         getAdminParams(),
                                                    plan)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
    
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, target);
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(target);
    }
}
