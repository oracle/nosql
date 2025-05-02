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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.plan.task.DeployDatacenter;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Topology;
import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.ObjectNode;

public class DeployDatacenterPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original parameters. */
    private final String datacenterName;

    private final DatacenterId datacenterId;
    private final Datacenter preexistingDC;

    /* Results */
    private final DatacenterParams newDCParams;

    // TODO the bulk of the deployment should be moved to the task, rather
    // than plan construction, so it happens dynamically.
    DeployDatacenterPlan(String planName,
                         Planner planner,
                         Topology topology,
                         String datacenterName,
                         int repFactor,
                         DatacenterType datacenterType,
                         boolean allowArbiters,
                         boolean masterAffinity) {
        super(planName, planner, topology);

        this.datacenterName = datacenterName;

        /* Error Checking */
        Rules.validateReplicationFactor(repFactor, datacenterType,
                                        allowArbiters);
        Rules.validateAllowArbiters(allowArbiters, datacenterType, repFactor);
        Rules.validateMasterAffinity(masterAffinity, datacenterType, repFactor);

        preexistingDC = getPreexistingDatacenter();
        if (preexistingDC != null) {
            checkPreexistingDCParams(repFactor, datacenterType, allowArbiters,
                                     masterAffinity);
            datacenterId = preexistingDC.getResourceId();
            newDCParams =
                planner.getAdmin().getCurrentParameters().get(datacenterId);
        } else {

            /* Add new data center to topology and create DataCenterParams */
            final Datacenter newDc = Datacenter.newInstance(datacenterName,
                                                            repFactor,
                                                            datacenterType,
                                                            allowArbiters,
                                                            masterAffinity);
            datacenterId = topology.add(newDc).getResourceId();
            newDCParams = new DatacenterParams(datacenterId, datacenterName);
        }

        /*
         * Create a deploy task, even if the topology and params are unchanged.
         * Right now, that deploy task will be meaningless, but in the future,
         * we may need to do some form of work even on a plan retry. Also,
         * the actual creation of the datacenter should be moved to task
         * run time, rather than being done at plan construction time.
         */
        addTask(new DeployDatacenter(this));
    }

    /** Returns the datacenter named by the plan, or null */
    private Datacenter getPreexistingDatacenter() {
        for (final Datacenter dc : getTopology().getDatacenterMap().getAll()) {
            if (dc.getName().equals(datacenterName)) {
                return dc;
            }
        }
        return null;
    }

    /**
     * Guard against a datacenter of the same name, but with different
     * attributes.
     *
     * @throw IllegalCommandException if params for this Datacenter already
     * exist, and are different from the new ones proposed
     */
    private void checkPreexistingDCParams(
        final int repFactor,
        final DatacenterType datacenterType,
        final boolean allowArbiters,
        final boolean masterAffinity) {

        if (preexistingDC.getRepFactor() != repFactor) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has a repFactor of " +
                preexistingDC.getRepFactor() +
                " rather than the requested repFactor of " +
                repFactor);
        }
        if (preexistingDC.getDatacenterType() != datacenterType) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has type " +
                preexistingDC.getDatacenterType() +
                " rather than the requested type " + datacenterType);
        }
        if (preexistingDC.getAllowArbiters() != allowArbiters) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has allowArbiters " +
                preexistingDC.getAllowArbiters() +
                " rather than the requested allowArbiters of " + allowArbiters);
        }
        if (preexistingDC.getMasterAffinity() != masterAffinity) {
            throw new IllegalCommandException(
                "Zone " + datacenterName +
                " already exists but has masterAffinity " +
                preexistingDC.getAllowArbiters() +
                " rather than the requested masterAffinity of " +
                masterAffinity);
        }
    }

    @Override
    void preExecutionSave() {

        /*
         * We only need to save the topology and params if this is the first
         * time the datacenter is created. It may be that we are retrying the
         * plan and that the topology and params are unchanged from what is
         * in the Admin db.
         */
        if (isFirstExecutionAttempt()) {
           getAdmin().saveTopoAndParams(getTopology(), getDeployedInfo(),
                                        newDCParams, this);
        }
    }

    @Override
    public String getDefaultName() {
        return "Deploy Zone";
    }

    public String getDatacenterName() {
        return datacenterName;
    }

    public int getRepFactor() {
        return getTopology().get(datacenterId).getRepFactor();
    }

    private DatacenterType getDatacenterType() {
        return getTopology().get(datacenterId).getDatacenterType();
    }

    private boolean getAllowArbiters() {
        return getTopology().get(datacenterId).getAllowArbiters();
    }

    private boolean getMasterAffinity() {
        return getTopology().get(datacenterId).getMasterAffinity();
    }

    @Override
    public String getOperation() {
        String arbiterOption =
            getAllowArbiters() ? " -arbiters" : " -no-arbiters";
        String masterAffinityOption =
            getMasterAffinity() ? " -master-affinity" : " -no-master-affinity";
        return "plan deploy-zone -name " + datacenterName +
               " -rf " + getRepFactor() +
               " -type " + getDatacenterType().name() +
               arbiterOption + masterAffinityOption;
    }

    /*
     * TODO: replace field names with constants held in a json/command output
     * utility class.
     */
    @Override
    public ObjectNode getPlanJson() {
        ObjectNode jsonTop = JsonUtils.createObjectNode();
        jsonTop.put("plan_id", getId());
        jsonTop.put("zone_name", datacenterName);
        jsonTop.put("zone_id", datacenterId.toString());
        jsonTop.put("type", getDatacenterType().name());
        jsonTop.put("rf", getRepFactor());
        jsonTop.put("allow_arbiters", getAllowArbiters());
        jsonTop.put("master_affinity", getMasterAffinity());
        return jsonTop;
    }
}
