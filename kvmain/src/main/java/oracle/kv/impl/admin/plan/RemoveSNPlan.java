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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.task.ConfirmSNStatus;
import oracle.kv.impl.admin.plan.task.RemoveSN;
import oracle.kv.impl.admin.plan.task.StopSN;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Remove a storage node permanently. Only permitted for storage nodes that
 * are not hosting any RNs or Admins.
 */
public class RemoveSNPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original inputs. */
    private final StorageNodeId target;


    public RemoveSNPlan(String planName,
                        Planner planner,
                        Topology topology,
                        StorageNodeId target) {

        super(planName, planner, topology);
        this.target = target;

        /*
         * Check the target exists and is not hosting any components.
         */
        validate();

        /* Stop the target Storage Node */
        addTask(new StopSN(this, target));

        /* Confirm that the old node is dead. */
        addTask(new ConfirmSNStatus(this,
                                    target,
                                    false /* shouldBeRunning*/,
                                    "Please ensure that " + target +
                                    " is stopped before attempting to remove " +
                                    "it."));
        addTask(new RemoveSN(this, target));
    }

    @Override
    public String getDefaultName() {
        return "Remove Storage Node";
    }
    private void validate() {

        Topology topology = getTopology();
        Parameters parameters = planner.getAdmin().getCurrentParameters();

        /* Confirm that the target exists in the params and topology. */
        if (topology.get(target) == null) {
            throw new IllegalCommandException
                (target + " does not exist in the topology and cannot " +
                 "be removed");
        }

        if (parameters.get(target) == null) {
            throw new IllegalCommandException
                (target + " does not exist in the parameters and cannot " +
                 "be migrated");
        }

        /*
         * This target should not host any services. Check the topology and
         * parameters for the presence of a RN or Admin.
         */
        List<ResourceId> existingServices = new ArrayList<>();

        for (AdminParams ap: parameters.getAdminParams()) {
            if (ap.getStorageNodeId().equals(target)) {
                existingServices.add(ap.getAdminId());
            }
        }

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(target)) {
                existingServices.add(rnp.getRepNodeId());
            }
        }

        if (existingServices.size() > 0) {
            throw new IllegalCommandException
                ("Cannot remove " + target + " because it hosts these " +
                 " components: " + existingServices);
        }
    }

    @Override
    public boolean updatingMetadata(Metadata<?> metadata) {
        if (metadata.getType().equals(MetadataType.SECURITY)) {
            final SecurityMetadata currentSecMd = this.getAdmin().
                getMetadata(SecurityMetadata.class, MetadataType.SECURITY);
            if (currentSecMd == null) {
                return true;
            }
            return metadata.getSequenceNumber() >
                currentSecMd.getSequenceNumber();
        }
        return false;
    }

    public StorageNodeId getTarget() {
        return target;
    }
}
