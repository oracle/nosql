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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.WriteNewANParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

public class ChangeANParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    protected ParameterMap newParams;
    public ChangeANParamsPlan(String name,
                              Planner planner,
                              Topology topology,
                              Set<ArbNodeId> anids,
                              ParameterMap map) {

        super(name, planner);

        if (anids.isEmpty()) {
            throw new IllegalCommandException(
                "Cannot change Arbiter parameters because there are " +
                "no arbiter nodes.");
        }

        this.newParams = map;
        /* Do as much error checking as possible, before the plan is executed.*/
        validateParams(newParams);

        generateTasksV2(anids, topology);
    }

    private void generateTasksV2(Set<ArbNodeId> anids, Topology topology) {
        for (ArbNodeId anid : anids) {
            StorageNodeId snid = topology.get(anid).getStorageNodeId();
            ParameterMap filtered = newParams.readOnlyFilter();
            addTask(new WriteNewANParams(this, filtered, anid, snid, true));
        }
    }

    protected List<ArbNodeId> sort(Set<ArbNodeId> ids,
    		@SuppressWarnings("unused") Topology topology) {
        List<ArbNodeId> list = new ArrayList<ArbNodeId>();
        for (ArbNodeId id : ids) {
            list.add(id);
        }
        return list;
    }

    protected void validateParams(ParameterMap map) {

        /* Check for incorrect JE params. */
        ParameterUtils.validateArbParams(map);
    }

    @Override
    public String getDefaultName() {
        return "Change ArbNode Params";
    }

    @Override
    public void stripForDisplay() {
        newParams = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
