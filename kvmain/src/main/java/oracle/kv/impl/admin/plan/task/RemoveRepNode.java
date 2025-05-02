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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for removing a RepNode from topology.
 *
 * Supports removing RNs when a shard is being removed during contraction,
 * including when removing a failed shard, and when reducing the rep factor for
 * a secondary zone.
 *
 * Step 1: Stop victim RepNode.
 * Step 2: Destroy the stopped victim from the SN which host it.
 * Step 3: Step 3: Remove RepNode and save changed plan.
 */
public class RemoveRepNode extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final RepNodeId victim;
    private final boolean failedShard;

    public RemoveRepNode(AbstractPlan plan,
                         RepNodeId victim,
                         boolean failedShard) {
        this.plan = plan;
        this.victim = victim;
        this.failedShard = failedShard;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public Callable<Task.State> getFirstJob(int taskId, TaskRunner runner) {
        return removeRepNodeJob(taskId, runner);
    }

    /**
     * @return a wrapper that will invoke to remove a RepNode.
     */
    private JobWrapper removeRepNodeJob(final int taskId,
                                        final TaskRunner runner) {
        return new JobWrapper(taskId, runner, "request removing a repnode") {
            @Override
            public NextJob doJob() {
                final Topology sourceTopo =
                        plan.getAdmin().getCurrentTopology();
                final RepGroupId rgId = new RepGroupId(victim.getGroupId());
                final RepGroup rg = sourceTopo.get(rgId);
                if (rg == null) {
                    /*
                     * This would happen if the plan was interrupted and
                     * re-executed.
                     */
                    plan.getLogger().log(Level.FINE,
                                         "{0} {1} does no exist",
                                         new Object[]{this, rg});
                    return NextJob.END_WITH_SUCCESS;
                }

                /*
                 * Check whether the to-be-removed shard has partitions. It's
                 * OK to remove an RN with data if it is a secondary node.
                 */
                final RepNode rn = sourceTopo.get(victim);
                /**
                 * Fix for KVSTORE-1844: NPE in RemoveRepNode.removeRepNodeJob
                 * This issue can happen if the next task RemoveShard fails
                 * and the plan is re-tried
                 */
                if (rn == null) {
                    return NextJob.END_WITH_SUCCESS;
                }
                final StorageNode sn = sourceTopo.get(rn.getStorageNodeId());
                final Datacenter dc = sourceTopo.get(sn.getDatacenterId());
                if (dc.getDatacenterType().isPrimary()) {
                    for (Partition partition :
                             sourceTopo.getPartitionMap().getAll()) {
                        if (partition.getRepGroupId().equals(rgId)) {
                            throw new IllegalStateException(
                                "Error removing " + victim +
                                ", shard is not empty of user data" +
                                " (partitions)");
                        }
                    }
                }

                return removeRepNode(taskId, runner);
            }
        };
    }

    /**
     * Remove a RepNode from topology and shard
     */
    private NextJob removeRepNode(int taskId, TaskRunner runner) {
        final Admin admin = plan.getAdmin();
        final AdminParams ap = admin.getParams().getAdminParams();

        /*
         * If failed shard is false, then we need to do all
         * three steps.
         *
         * For failed-shard true case :
         * If victim belongs to the failedShard then no need to
         * stop the victim i.e. step 1 just update admin params in
         * stopRN.
         *
         * If we have disk failures in case of capacity >= 1 then
         * we just need to remove Parameter Map component specific
         * to failed RN's from StorageNode.
         *
         * Since data for failed RN has already been lost then
         * no need to delete data in either case whether we
         * have capacity > 1 or = 1.
         *
         * We need to do step 3 for both capacity =1 or >1 case
         * and then return.
         *
         * Step 1: Stop victim RepNode.
         * Step 2: Destroy the stopped victim from the SN which host it.
         * Step 3: Remove RepNode and save changed plan.
         */
        final Topology topo = admin.getCurrentTopology();
        final RepNode rn = topo.get(victim);
        final StorageNodeId snId = rn.getStorageNodeId();

        /*
         * Before proceeding with removeRepNode, check running status
         * of victim again in case of failed shard.
         */
        if (failedShard) {
            Map<ResourceId, ServiceChange> statusMap =
                admin.getMonitor().getServiceChangeTracker().getStatus();
            final ServiceChange change = statusMap.get(victim);
            if (ServiceStatus.RUNNING.equals(change.getStatus())) {
                /* RepNode is running hence error out and don't
                 * proceed further.
                 */
                throw new IllegalStateException
                    ("Error removing failed RepNode " + victim +
                     ", RepNode is found running");
            }
        }

        try {
            /* Step 1: Transfer master to another RepNode removeId. */
            Utils.stopRN(plan, snId, victim,
                    false, /* not await for healthy */
                    failedShard);
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try again later. */
            return new NextJob(Task.State.RUNNING,
                               removeRepNodeJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());
        }

        try {
            /* Step 2: Destroy the stopped victim from
             * the SN which host it.
             */
            final RegistryUtils registry =
                    new RegistryUtils(topo, admin.getLoginManager(),
                                      plan.getLogger());
            StorageNodeAgentAPI oldSna = null;
            try {
                oldSna = registry.getStorageNodeAgent(snId);
            } catch (RemoteException | NotBoundException e) {
                /*
                 * OK if the SNA isn't available in the failed shard case --
                 * the SNA parameters can be repaired later.
                 */
                if (!failedShard) {
                    throw e;
                }
            }
            if (oldSna != null) {
                oldSna.destroyRepNode(victim,
                                     ((failedShard) ?
                                         false /*data already lost*/:
                                         true /* deleteData */),
                                      plan.toString());
            }
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try again later. */
            return new NextJob(Task.State.RUNNING,
                               removeRepNodeJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());
        }

        /* Step 3: Remove RepNode and save changed plan. */
        admin.removeRepNodeAndSaveTopo(victim,
                                       plan.getDeployedInfo(),
                                       plan);

        return NextJob.END_WITH_SUCCESS;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(victim);
    }
}
