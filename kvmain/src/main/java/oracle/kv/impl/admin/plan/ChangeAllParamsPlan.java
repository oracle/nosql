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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Change parameters store-wide for all RepNodes.
 *
 * The UI should filter based on parameters available for store-wide
 * modification but it's done here as well, just in case.
 *
 * Note that the sort method will only be called if there are restart-required
 * parameters to be modified.
 */
public class ChangeAllParamsPlan extends ChangeParamsPlan {

    private static final long serialVersionUID = 1L;

    public ChangeAllParamsPlan(String name,
                               Planner planner,
                               Topology topology,
                               DatacenterId dcid,
                               ParameterMap map) {

        /* Filter params based on store-wide scope just in case */
        super(name, planner, topology,
              topology.getRepNodeIds(dcid),
              map.filter(ParameterState.Scope.STORE));
    }

    /**
     * Sort for optimal restart order.  This may change based on experience.
     * Current order:
     *  o restart replicas first, in no particular order
     *  o restart masters, in no particular order
     * This means that the sort just does a ping and separates replicas and
     * masters.
     */
    @Override
    protected List<RepNodeId> sort(Set<RepNodeId> ids, Topology topology) {
        int replicaIndex = 0;
        int masterIndex = ids.size() - 1;
        RegistryUtils ru =
            new RegistryUtils(topology, getLoginManager(), logger);
        RepNodeId[] list = new RepNodeId[ids.size()];
        for (RepNodeId id : ids) {
            if (isReplica(id, ru)) {
                list[replicaIndex++] = id;
            } else {
                list[masterIndex--] = id;
            }
        }
        return Arrays.asList(list);
    }

    /**
     * Try to ping the service.  If it is not available, log the fact and add
     * the id to the end of the list.  If it's still not available when the
     * plan is executed the user will see that.
     */
    private boolean isReplica(RepNodeId id, RegistryUtils ru) {
        Exception e = null;
        try {
            RepNodeAdminAPI rna = ru.getRepNodeAdmin(id);
            RepNodeStatus status = rna.ping();
            return status.getReplicationState().isReplica();
        } catch (RemoteException re) {
            e = re;
        } catch (NotBoundException nbe) {
            e = nbe;
        }
        planner.getLogger().warning("Could not reach node " + id +
                                    " in ChangeAllParamsPlan: " + e);
        return false;
    }
}
