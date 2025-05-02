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

import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Task for creating and starting all RepNodes and ArbNodes which are
 * housed on a particular Storage Node.
 */
public class DeployMultipleRNs extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected final AbstractPlan plan;
    protected final StorageNodeId snId;
    protected final String snDescriptor;

    /**
     * Creates a task for creating and starting a new RepNode.
     */
    public DeployMultipleRNs(AbstractPlan plan,
                             StorageNodeId snId) {
        super();
        this.plan = plan;
        this.snId = snId;

        /* A more descriptive label used for error messages, etc. */
        final StorageNodeParams snp =
                plan.getAdmin().getStorageNodeParams(snId);
        snDescriptor = snp.displaySNIdAndHost();
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    private Set<RepNodeId> getTargets() {
        final Parameters parameters = plan.getAdmin().getCurrentParameters();
        final Set<RepNodeId> targetSet = new HashSet<>();

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snId)) {
                targetSet.add(rnp.getRepNodeId());
            }
        }
        return targetSet;
    }

    private Set<ArbNodeId> getANTargets() {
        final Parameters parameters = plan.getAdmin().getCurrentParameters();
        final Set<ArbNodeId> targetSet = new HashSet<>();

        for (ArbNodeParams anp: parameters.getArbNodeParams()) {
            if (anp.getStorageNodeId().equals(snId)) {
                targetSet.add(anp.getArbNodeId());
            }
        }
        return targetSet;
    }

    @Override
    public State doWork()
        throws Exception {
        final Admin admin = plan.getAdmin();

        /*
         * Attempt to deploy repNodes. Note that one or more repNodes may have
         * been previously deployed. The task is successful if the repNodes are
         * created, or are on their way to coming up.
         */
        final Topology topo = plan.getAdmin().getCurrentTopology();
        final ParameterMap policyMap =
                plan.getAdmin().getCurrentParameters().copyPolicies();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginMgr, plan.getLogger());
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
        final StorageNodeParams snp =
                plan.getAdmin().getCurrentParameters().get(snId);
        final int gcThreads = snp.calcGCThreads();
        final Set<RepNodeId> targetRNIds = getTargets();
        final int numRNsOnSN = targetRNIds.size();
        final int numANsOnSN = topo.getHostedArbNodeIds(snId).size();


        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                                            Utils.getMetadataSet(topo, plan);

        for (RepNodeId rnId : targetRNIds) {
            final RepNodeParams rnp = admin.getRepNodeParams(rnId);
            final RNHeapAndCacheSize heapAndCache =
                snp.calculateRNHeapAndCache(policyMap,
                                            numRNsOnSN,
                                            rnp.getRNCachePercent(),
                                            numANsOnSN);
            rnp.setRNHeapAndJECache(heapAndCache);
            rnp.setParallelGCThreads(gcThreads);

            /* Set the concurrent threads the same as parallel threads */
            rnp.setConcurrentGCThreads(gcThreads);
            sna.createRepNode(rnp.getMap(), metadataSet);

            /* Register this repNode with the monitor. */
            final StorageNode sn = topo.get(snId);
            admin.getMonitor().registerAgent(sn.getHostname(),
                                             sn.getRegistryPort(),
                                             rnId);
        }

        final Set<ArbNodeId> targetANIds = getANTargets();
        for (ArbNodeId anId : targetANIds) {
            final ArbNodeParams anp = admin.getArbNodeParams(anId);
            sna.createArbNode(anp.getMap());

            /* Register this arbNode with the monitor. */
            final StorageNode sn = topo.get(snId);
            admin.getMonitor().registerAgent(sn.getHostname(),
                                             sn.getRegistryPort(),
                                             anId);
        }


        /*
         * At this point, we've succeeded. The user will have to rely on ping
         * and on monitoring to wait for all the rep nodes to come up.
         */
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        
        for (RepNodeId rnId : getTargets()) {
            LockUtils.lockRG(planner, plan, new RepGroupId(rnId.getGroupId()));
        }
    }
}
