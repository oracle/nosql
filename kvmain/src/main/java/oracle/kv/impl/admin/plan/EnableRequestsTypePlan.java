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
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.WriteNewRequestType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;

/**
 * Enable specify request type on given shards or the entire store.
 */
public class EnableRequestsTypePlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;
    private final Set<RepNode> rns = new HashSet<>();

    public EnableRequestsTypePlan(final String planName,
                                  final Planner planner,
                                  final String requestType,
                                  final Set<? extends ResourceId> resourceIds,
                                  final boolean entireStore) {
        super(planName, planner);
        final Admin admin = planner.getAdmin();

        WriteNewRequestType.validateRequestType(requestType);

        if (entireStore && (resourceIds != null && !resourceIds.isEmpty())) {
            throw new IllegalCommandException(
                "Cannot enable request type of specific shard and " +
                "the entire store at the same time");
        }

        /* Find target RepNode IDs to enable their request type */
        final Topology topo = admin.getCurrentTopology();
        if (entireStore) {
            rns.addAll(topo.getSortedRepNodes());
        } else {
            if (resourceIds == null || resourceIds.isEmpty()) {
                throw new IllegalCommandException(
                    "To change enabled requests type, must " +
                    "specify store or shard IDs");
            }
            for (ResourceId resourceId : resourceIds) {

                /*
                 * Using arbitrary type of resource IDs in case to support
                 * configure enabled request type for individual RNs in future,
                 * for now, only RepGroupID is allowed.
                 */
                if (!resourceId.getType().isRepGroup()) {
                    throw new IllegalCommandException(
                        "Only can enable request type of shards or the " +
                        "whole store, specifying invalid shard ID " +
                        resourceId);
                }
                final RepGroup repGroup =
                    topo.getRepGroupMap().get((RepGroupId) resourceId);
                rns.addAll(repGroup.getRepNodes());
            }
        }

        /* Change enabled request type for RepNodes */
        for (RepNode rn : rns) {
            addTask(new WriteNewRequestType(
                this, requestType, rn.getResourceId(), rn.getStorageNodeId()));

            /*
             * Request type stored in parameters, call this task to ask
             * RN refresh parameter in order to reflect the change.
             */
            addTask(new NewRepNodeParameters(this, rn.getResourceId()));
        }
    }

    @Override
    public String getDefaultName() {
        return "Enable Requests Type";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
