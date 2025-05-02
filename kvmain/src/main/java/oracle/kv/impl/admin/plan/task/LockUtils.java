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

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.Planner.LockCategory;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Utility methods implementing task locking.
 */
public class LockUtils {

    private LockUtils() { /* Prevent construction */ }

    /* -- Topology locking -- */

    /**
     * Locks the specified resource which must be an rep node or arbiter. An
     * exclusive lock is taken on the resource while a read lock is taken on
     * the group the resource belongs to. Throws PlanLocksHeldException if
     * either lock cannot be acquired.
     */
    static void lockANRN(Planner planner, Plan plan, ResourceId resourceId)
            throws PlanLocksHeldException {
        assert (resourceId instanceof RepNodeId) ||
               (resourceId instanceof ArbNodeId);
        final RepGroupId rgId = Utils.getRepGroupId(resourceId);
        assert rgId != null;
        planner.lock(plan.getId(), plan.getName(), LockCategory.TOPO,
                     rgId.getFullName(), resourceId.getFullName());
    }

    /**
     * Locks the specified rep node. An exclusive lock is taken on the RN while
     * a read lock is taken on the group the RN belongs to. Throws
     * PlanLocksHeldException if either lock cannot be acquired.
     */
    static void lockRN(Planner planner, Plan plan, RepNodeId rnId)
            throws PlanLocksHeldException {
        lockANRN(planner, plan, rnId);
    }

    /**
     * Locks the specified arbiter. An exclusive lock is taken on the AN while
     * a read lock is taken on the group the AN belongs to. Throws
     * PlanLocksHeldException if either lock cannot be acquired.
     */
    static void lockAN(Planner planner, Plan plan, ArbNodeId anId)
            throws PlanLocksHeldException {
        lockANRN(planner, plan, anId);
    }

    /**
     * Locks the specified rep group.
     */
    static void lockRG(Planner planner, Plan plan, RepGroupId rgId)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.TOPO, rgId.getFullName());
    }

    /**
     * Locks the specified SN.
     */
    public static void lockSN(Planner planner, Plan plan, StorageNodeId snId)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.TOPO, snId.getFullName());
    }

    /**
     * Locks the specified Admin.
     */
    static void lockAdmin(Planner planner, Plan plan, AdminId aId)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.TOPO, aId.getFullName());
    }

    /* -- Table locking -- */

    /**
     * Locks the specified table.
     */
    static void lockTable(Planner planner, Plan plan,
                          String namespace, String tableName)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.TABLE, namespace, tableName);

    }

    /**
     * Locks the specified index. A read lock is taken in the table.
     */
    static void lockIndex(Planner planner, Plan plan,
                          String namespace, String tableName, String indexName)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.TABLE, namespace, tableName, indexName);
    }

    /**
     * Locks the specified namespace.
     */
    static void lockNamespace(Planner planner, Plan plan, String namespace)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.TABLE, namespace);

    }

    /**
     * Locks the specified region.
     */
    static void lockRegion(Planner planner, Plan plan, String regionName)
            throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(),
                     LockCategory.REGION, regionName);

    }
}
