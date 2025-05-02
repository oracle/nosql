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

import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.DeployAdmin;
import oracle.kv.impl.admin.plan.task.UpdateAdminHelperHost;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.plan.task.WaitForAdminState;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.ObjectNode;

/**
 * A plan for deploying one or more instances of the Admin service onto
 * storage nodes.
 */
public class DeployAdminPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;
    
    /*
     * httpPort is no longer used, but it is kept here
     * for serialization compatibility.
     */
    @SuppressWarnings("unused")
    @Deprecated
    private int httpPort;
    AdminParams targetAP;

    public DeployAdminPlan(String name,
                           Planner planner,
                           StorageNodeId snid,
                           AdminType type) {

        super(name, planner);

        targetAP = createAdminParams(snid, type);
        AdminId newAdminId = targetAP.getAdminId();
        addTask(new DeployAdmin(this, snid, newAdminId));
        addTask(new WaitForAdminState(this, snid, newAdminId,
                                      ServiceStatus.RUNNING));
        addTask(new UpdateAdminHelperHost(this));
    }
    
    private AdminParams checkAndFindAdminParams(StorageNodeId snid,
                                                AdminType type) {

        Admin admin = planner.getAdmin();

        final Topology topo = admin.getCurrentTopology();
        final StorageNode sn = topo.get(snid);
        if (sn == null) {
            throw new IllegalCommandException
                (snid + " is not a valid Storage Node id.  " +
                 "Please provide the id of an existing Storage Node.");
        }

        Datacenter dc = topo.getDatacenter(snid);
        if (dc.getRepFactor() == 0) {
            throw new IllegalCommandException("Cannot create Admin on " +
                "Storage Node in zero replication factor zone.");
        }

        type = computeAdminType(type, topo, snid);
        return findExistingParams(admin, snid, type);
    }

    private AdminParams createAdminParams(StorageNodeId snid,
                                          AdminType type) {

        AdminParams ap = null;
        Admin admin = planner.getAdmin();

        ap = checkAndFindAdminParams(snid, type);
        if (ap == null) {
            ParameterMap pMap = admin.copyPolicy();
            type = computeAdminType(type, admin.getCurrentTopology(),snid);
            final StorageNodeParams snParams =
                     admin.getParams().getStorageNodeParams(); 
            if (snParams.getStorageNodeId().equals(snid)) {
                final AdminParams adminParams = 
                     admin.getParams().getAdminParams();
                ap = new AdminParams(pMap, admin.generateAdminId(), snid,
                                       type, adminParams.getAdminStorageDir());
            } else {
                ap = new AdminParams(pMap, admin.generateAdminId(), snid,
                                       type, snParams.getRootDirPath());
            } 
               
            removeSomePolicyParams(ap.getMap());
        } 
        return ap;
    }

    /*
     * Returns the type for creating an Admin on the specified SN. If the
     * stecified type is null, the returned type will be the same as the zone
     * in which the Admin will reside. Otherwise type is returned.
     */
    private AdminType computeAdminType(AdminType type,
                                       Topology topo,
                                       StorageNodeId snId) {
        if (type != null) {
            return type;
        }
        /* If the type is null use the zone type */
        final DatacenterType dcType =
                                topo.getDatacenter(snId).getDatacenterType();
        return Utils.getAdminType(dcType);
    }

    /*
     * Returns any Admin params that exist on this storage node already. Also
     * verifies that at least one Admin is configured as a primary. Throws an
     * IllegalCommandException if none are found. This means that the first
     * Admin configured must be a primary.
     */
    private AdminParams findExistingParams(Admin admin,
                                           StorageNodeId snid,
                                           AdminType type) {
        final Parameters parameters = admin.getCurrentParameters();
        boolean primaryConfigured = type.isPrimary();
        AdminParams existingAP = null;
        for (AdminId aid : parameters.getAdminIds()) {
            AdminParams ap = parameters.get(aid);
            if (snid.equals(ap.getStorageNodeId())) {
                if (existingAP != null) {
                    /* Should only be one Admin on any SN. */
                    throw new NonfatalAssertionException
                        ("More than one admin service exists on " + snid +
                         ". " + existingAP.getAdminId() + " and " +
                         ap.getAdminId());
                }

                if (!ap.getType().equals(type)) {
                    throw new IllegalCommandException("Attempting to change " +
                                                      aid  + " from " +
                                                      ap.getType() + " to " +
                                                      type);
                }
                existingAP = ap;
            }
            if (ap.getType().isPrimary()) {
                primaryConfigured = true;
            }
        }
        if (!primaryConfigured) {
            throw new IllegalCommandException("No primary Admin configured");
        }
        return existingAP;
    }

    /*
     * Do not use these as policy params for the admin:
     * JE_CACHE_SIZE, JE_MISC, JVM_MISC, JE_ENABLE_ERASURE, JE_ERASURE_PERIOD.
     */
    private void removeSomePolicyParams(ParameterMap map) {
        map.remove(ParameterState.JE_CACHE_SIZE);
        map.remove(ParameterState.JE_MISC);
        map.remove(ParameterState.JVM_MISC);
        map.remove(ParameterState.JE_ENABLE_ERASURE);
        map.remove(ParameterState.JE_ERASURE_PERIOD);
    }

    @Override
    public void preExecutionSave() {
        Admin admin = getAdmin();

        /* We can only "add" the new Admin once. */
        Parameters p = admin.getCurrentParameters();
        if (p.get(targetAP.getAdminId()) == null) {
            admin.addAdminParams(targetAP);
        }
    }
    
    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        Admin admin = getAdmin();

        Parameters p = admin.getCurrentParameters();
        if (p.get(targetAP.getAdminId()) == null) {
            /* check to insure sn still exists
             * and datacenter type was not altered */
            checkAndFindAdminParams(targetAP.getStorageNodeId(), 
                                    targetAP.getType());
        }
    }

    @Override
    public String getDefaultName() {
        return "Deploy Admin Service";
    }

    @Override
    public void stripForDisplay() {
        targetAP = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
    
    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    @Override
    public String getOperation() {
        return "plan deploy-admin -sn " +
               targetAP.getStorageNodeId().getStorageNodeId();
    }

    /*
     * TODO: replace field names with constants held in a json/command output
     * utility class.
     */
    @Override
    public ObjectNode getPlanJson() {
        ObjectNode jsonTop = JsonUtils.createObjectNode();
        jsonTop.put("plan_id", getId());
        jsonTop.put("resource_id", targetAP.getAdminId().toString());
        jsonTop.put("sn_id", targetAP.getStorageNodeId().toString());
        return jsonTop;
    }
}
