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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.NewAdminParameters;
import oracle.kv.impl.admin.plan.task.StartAdminV2;
import oracle.kv.impl.admin.plan.task.StopAdmin;
import oracle.kv.impl.admin.plan.task.WaitForAdminState;
import oracle.kv.impl.admin.plan.task.WriteNewAdminParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

public class ChangeAdminParamsPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;

    private ParameterMap newParams;

    /*
     * An entry in the needsAction set is made by WriteNewAdminParams for the
     * AdminId associated with the task, if the task indeed modified the
     * parameters.
     *
     * Subsequent shutdown and restart tasks check the for the presence of
     * their associated AdminId to determine whether or not they need to take
     * action.
     */
    private transient Set<AdminId> needsAction;

    public ChangeAdminParamsPlan(String name,
                                 Planner planner,
                                 AdminId givenId,
                                 ParameterMap map) {

        this(name, planner, givenId, null, null, map);
    }

    public ChangeAdminParamsPlan(String name,
                                 Planner planner,
                                 AdminId givenId,
                                 DatacenterId dcid,
                                 Topology topology,
                                 ParameterMap map) {

        super(name, planner);

        newParams = map;
        validateParams(newParams);

        Admin admin = planner.getAdmin();
        Parameters parameters = admin.getCurrentParameters();

        Set<AdminId> allAdmins;
        /* If the givenID is null, make a plan to change ALL admins. */
        if (givenId == null) {
            allAdmins = parameters.getAdminIds(dcid, topology);
        } else {
            allAdmins = new HashSet<>();
            allAdmins.add(givenId);
        }

        Set<AdminId> restartIds = new HashSet<>();

        ParameterMap filtered = newParams.readOnlyFilter();

        for (AdminId aid : allAdmins) {
            AdminParams current = parameters.get(aid);
            StorageNodeId snid = current.getStorageNodeId();
            StorageNodeParams snp = parameters.get(snid);
            String hostname = snp.getHostname();
            int registryPort = snp.getRegistryPort();
            addTask(new WriteNewAdminParams(this, filtered, aid, snid));

            if (filtered.hasRestartRequiredDiff(current.getMap())) {
                restartIds.add(aid);
            } else {
                addTask
                    (new NewAdminParameters(this, hostname, registryPort, aid));
            }
        }

        if (!restartIds.isEmpty()) {
            AdminId self = admin.getParams().getAdminParams().getAdminId();
            boolean restartSelf = false;

            for (AdminId aid : restartIds) {
                /* Check for this Admin instance. */
                if (aid.equals(self)) {
                    restartSelf = true;
                    continue; /* Don't restart self, yet. */
                }
                addRestartTasks(aid, parameters);
            }

            /* Do self last. */
            if (restartSelf) {
                addRestartTasks(self, parameters);
            }
        }
    }

    private void addRestartTasks(AdminId aid, Parameters parameters) {
        AdminParams current = parameters.get(aid);
        StorageNodeId snid = current.getStorageNodeId();
        addTask(new StopAdmin(this, snid, aid, false));
        addTask(new StartAdminV2(this, snid, aid, false));
        addTask(new WaitForAdminState(this, snid, aid, ServiceStatus.RUNNING));
    }

    private void validateParams(ParameterMap map) {

        /* Check for incorrect JE params. */
        ParameterUtils.validateParams(map);
    }

    @Override
    public String getDefaultName() {
        return "Change Admin Params";
    }

    public AdminParams getNewParams() {
        return new AdminParams(newParams);
    }

    public Set<AdminId> getNeedsActionSet() {
        return needsAction;
    }

    public void interrupt() {
        planner.interruptPlan(getId());
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

    @Override
    protected synchronized void initTransientFields() {
        super.initTransientFields();
        needsAction = new HashSet<>();
    }
}
