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
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.admin.topo.TopologyBuilder;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Topology;

/**
 * A task for updating the replication factor of a datacenter
 */
public class UpdateDatacenter extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected final DatacenterId dcId;
    protected final int newRepFactor;
    protected final DeployTopoPlan plan;

    protected UpdateDatacenter(DeployTopoPlan plan, DatacenterId dcId,
                               int newRepFactor) {
        super();
        this.plan = plan;
        this.dcId = dcId;
        this.newRepFactor = newRepFactor;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    /*
     * Update the repfactor for this datacenter.
     */
    @Override
    public State doWork()
        throws Exception {

        final Topology current = plan.getAdmin().getCurrentTopology();
        final Datacenter currentdc = current.get(dcId);

        if (checkRF(currentdc)) {
            current.update(dcId,
                       Datacenter.newInstance(currentdc.getName(),
                                              newRepFactor,
                                              currentdc.getDatacenterType(),
                                              currentdc.getAllowArbiters(),
                                              currentdc.getMasterAffinity()));
            plan.getAdmin().saveTopo(current, plan.getDeployedInfo(), plan);
        }
        return Task.State.SUCCEEDED;
    }

    /**
     * Returns true if there is a change in RF.
     *
     * @throws IllegalCommandException if the new RF is less than the current
     * RF
     */
    protected boolean checkRF(Datacenter currentdc) {
        return TopologyBuilder.checkNewRepFactor(currentdc, newRepFactor);
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" zone=").append(dcId);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    public static class UpdateDatacenterV2 extends UpdateDatacenter {
        private static final long serialVersionUID = 1L;

        protected final DatacenterType newType;
        private final boolean newAllowArbiters;
        private final boolean newMasterAffinity;

        public UpdateDatacenterV2(DeployTopoPlan plan, DatacenterId dcId,
                                  int newRepFactor, DatacenterType newType,
                                  boolean newAllowArbiters,
                                  boolean newMasterAffinity) {
            super(plan, dcId, newRepFactor);
            this.newType = newType;
            this.newAllowArbiters = newAllowArbiters;
            this.newMasterAffinity = newMasterAffinity;
        }

        /*
         * Update the repfactor, type and/or allowArbiters of this datacenter.
         */
        @Override
        public State doWork()
            throws Exception {

            final Admin admin = plan.getAdmin();
            final Topology current = admin.getCurrentTopology();
            final Datacenter currentdc = current.get(dcId);

             if (checkRF(currentdc) ||
                 !currentdc.getDatacenterType().equals(newType) ||
                 currentdc.getAllowArbiters() != newAllowArbiters ||
                 currentdc.getMasterAffinity() != newMasterAffinity) {
                 final Datacenter newdc =
                     Datacenter.newInstance(currentdc.getName(),
                                            newRepFactor,
                                            newType,
                                            newAllowArbiters,
                                            newMasterAffinity);
                current.update(dcId, newdc);
                admin.saveTopo(current, plan.getDeployedInfo(), plan);
            }
            return Task.State.SUCCEEDED;
        }
    }
}
