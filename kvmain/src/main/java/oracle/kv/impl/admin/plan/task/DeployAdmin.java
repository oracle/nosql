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

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for creating and starting an instance of an Admin service on a
 * StorageNode.
 */
public class DeployAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<Integer> FAULT_HOOK;

    private final AbstractPlan plan;
    private final AdminId adminId;
    private final StorageNodeId snId;

    public DeployAdmin(AbstractPlan plan,
                       StorageNodeId snId,
                       AdminId adminId) {
        super();
        this.plan = plan;
        this.snId = snId;
        this.adminId = adminId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();

        final Topology topology = admin.getCurrentTopology();
        final Parameters parameters = admin.getCurrentParameters();

        /*
         * If needed, set a helper host and nodeHostPort for this Admin.
         */
        final AdminParams ap = parameters.get(adminId);
        if (ap.getHelperHosts() == null) {
            initParams(topology, parameters, admin, ap);
        }
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 1);

        /* Now ask the SNA to create it. */
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils registryUtils =
                new RegistryUtils(topology, loginMgr, plan.getLogger());
        final StorageNodeAgentAPI sna = registryUtils.getStorageNodeAgent(snId);
        sna.createAdmin(ap.getMap());
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 2);

        return State.SUCCEEDED;
    }

    private void initParams(Topology topology,
                            Parameters parameters,
                            Admin admin,
                            AdminParams ap) {

        String nodeHostPort;
        String helperHost;
        if (adminId.getAdminInstanceId() == 1) {

            /*
             * If this is the first admin to be deployed, it already has a
             * NodeHostPort setting.  Its helperHost should be itself.
             */
            nodeHostPort = admin.getParams().getAdminParams().getNodeHostPort();
            helperHost = nodeHostPort;

        } else {
            /*
             * If this is a secondary admin deployment, we'll ask the
             * PortTracker to give out a HA port number for it.
             */
            final PortTracker portTracker = new PortTracker(topology,
                                                            parameters,
                                                            snId);

            final String haHostName = parameters.get(snId).getHAHostname();
            final int haPort = portTracker.getNextPort(snId);
            nodeHostPort = haHostName + ":" + haPort;

            /* Assemble the list of helper hosts */
            helperHost = Utils.findAdminHelpers(parameters, adminId);
        }

        ap.setJEInfo(nodeHostPort, helperHost);

        /* Duplicates the dns cache ttl values. */
        final StorageNodeParams snp = parameters.get(snId);
        ap.duplicateCacheTTLValues(snp.getMap());

        plan.getAdmin().updateParams(ap);
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" ").append(adminId)
                                .append(" on ").append(snId);
    }
    
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, snId);
    }

    public StorageNodeId getSnId() {
        return snId;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
