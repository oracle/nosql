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
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Stop Storage Node
 */
public class StopSN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final StorageNodeId target;
    private final TopologyPlan plan;

    public StopSN(TopologyPlan plan,
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

        /* Get StorageNodeAgentAPI and shut down the Storage Node directly */
        final Admin admin = plan.getAdmin();
        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(),
                              plan.getLogger());
        try {
            final StorageNodeAgentAPI sna =
                    registryUtils.getStorageNodeAgent(target);
            sna.shutdown(true /* stopService */, false /* force */,
                         plan.toString());
        } catch (NotBoundException | RemoteException e) {
            plan.getLogger().log(Level.FINE,
                                 "{0} the SN cannot be found by RMI", this);
        }
        return Task.State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
