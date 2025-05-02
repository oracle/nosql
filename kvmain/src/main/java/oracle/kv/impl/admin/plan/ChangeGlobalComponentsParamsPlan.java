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

package oracle.kv.impl.admin.plan;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.NewAdminGlobalParameters;
import oracle.kv.impl.admin.plan.task.NewGlobalParameters;
import oracle.kv.impl.admin.plan.task.NewRNGlobalParameters;
import oracle.kv.impl.admin.plan.task.PostStoreVersion;
import oracle.kv.impl.admin.plan.task.WriteNewGlobalParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * A plan for changing global parameters for components.
 *
 * TODO: Should this be merged with ChangeGlobalSecurityParamsPlan?
 */
public class ChangeGlobalComponentsParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private ParameterMap newParams = null;
    private Parameters currentParams;

    public ChangeGlobalComponentsParamsPlan(String name,
                                            Planner planner,
                                            Topology topology,
                                            ParameterMap map,
                                            KVVersion storeVersion) {
        /* If storeVersion is set, this is a system plan */
        super(name, planner, storeVersion != null /*systemPlan*/);

        this.newParams = map;
        Admin admin = planner.getAdmin();
        currentParams = admin.getCurrentParameters();

        /* global parameters, but filter out read-only, security parameters */
        final ParameterMap filtered = newParams.readOnlyFilter().
            filter(EnumSet.of(ParameterState.Info.GLOBAL)).
            filter(ParameterState.Info.SECURITY, false);

        final GlobalParams currentGlobalParams =
            currentParams.getGlobalParams();
        final boolean needsRestart =
            filtered.hasRestartRequiredDiff(currentGlobalParams.getMap());

        /* There should be no restart required */
        if (needsRestart) {
            throw new NonfatalAssertionException(
                "Parameter change would require an admin restart, which is " +
                "not supported.");
        }

        final List<StorageNodeId> snIds = topology.getStorageNodeIds();
        for (final StorageNodeId snId : snIds) {

            /*
             * First write the new global components parameters on all storage
             * nodes
             */
            addTask(new WriteNewGlobalParams(this, filtered, snId,
                                             true /* continuePastError */));

            addNewGlobalParametersTasks(snId, topology);
        }

        /*
         * If the store version was specified post the version to the MR agent.
         * Note that the admins must be at the version that supports
         * posting the version.
         */
        if (storeVersion != null) {
            /*
             * NOTE: The semantics of the PostStoreVersion task has changed
             * because the semantics of the store version message has changed.
             * PostStoreVersion now updates the minimum agent version if
             * possible. The minimum agent version is capped at the store
             * version so we attempt to update it any time the store version
             * is updated.
             */
            addTask(new PostStoreVersion(this, storeVersion));
        }
    }

    /*
     * Add newGlobalParameter tasks for all components in the specified storage
     * node, including Admin and RepNode services
     */
    private void addNewGlobalParametersTasks(final StorageNodeId snId,
                                             final Topology topo) {

        final Set<RepNodeId> refreshRns = topo.getHostedRepNodeIds(snId);
        for (final RepNodeId rnid : refreshRns) {
            addTask(new NewRNGlobalParameters(this, rnid));
        }
        for (final ArbNodeId anId : topo.getHostedArbNodeIds(snId)) {
            addTask(new NewGlobalParameters(this, anId));
        }

        for (final AdminId aid : currentParams.getAdminIds()) {
            final StorageNodeId sidForAdmin =
                currentParams.get(aid).getStorageNodeId();
            if (sidForAdmin.equals(snId)) {
                final StorageNodeParams snp = currentParams.get(sidForAdmin);
                final String hostname = snp.getHostname();
                final int registryPort = snp.getRegistryPort();

                addTask(new NewAdminGlobalParameters(
                    this, hostname, registryPort, aid));
            }
        }
    }

    @Override
    public String getDefaultName() {
        return "Change Global Params";
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    @Override
    public void stripForDisplay() {
        newParams = null;
        currentParams = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
