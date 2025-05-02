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
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.task.ChangeServiceAddresses;
import oracle.kv.impl.admin.plan.task.ConfirmSNStatus;
import oracle.kv.impl.admin.plan.task.DeployAdmin;
import oracle.kv.impl.admin.plan.task.DeployMultipleRNs;
import oracle.kv.impl.admin.plan.task.MigrateParamsAndTopo;
import oracle.kv.impl.admin.plan.task.NewArbNodeParameters;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.UpdateAdminHelperHost;
import oracle.kv.impl.admin.plan.task.UpdateHelperHostV2;
import oracle.kv.impl.admin.plan.task.VerifyBeforeMigrate;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * A plan for moving all the services from one storage node to another node in
 * the KV Store.
 *
 * We currently require that the destination node does not host any other
 * services. This precludes the ability to move services from a defunct storage
 * node onto one that is already part of the store. For example, suppose the
 * store is currently deployed on storage nodes 1 - 100, and suppose that
 * storage node 2 (sn2) fails. The MigrateSNPlan requires that sn2 is replaced
 * with a new node, such as sn101. We don't provide the ability to move sn2's
 * services to sn1, sn3-100.
 *
 * If we did permit sn2's services to move to an existing node, the kvstore
 * would become imbalanced, in terms of performance. We would also have to
 * decide whether we were willing to reduce availability by letting two rep
 * nodes from the same group exist on a single storage node. And finally, if we
 * did permit this, we would have to provide some way to repair the situation,
 * and move these relocated services back to some other sn when one became
 * available.
 *
 * For this reason, it is a conscious decision to avoid supporting that case
 * for R1.
 *
 * R1 note: The user must let the MigrateSNPlan succeed. If it does not, she
 * will have to also replace the failed destination. For example, suppose she
 * attempts to replace sn1 with sn77. If that plan has some fatal flaw (i.e a
 * problem with sn77), the metadata may have been transfered, but the
 * deployments will not succeed. In that case, she must cancel the {@literal
 * sn1->sn77} plan, and and replace sn77 with the some other sn.
 *
 * R3 notes:
 * - It's now our goal to be able to recover from an incomplete
 * MigrateSNPlan. The user should be able to restart sn1 and repair the
 * topology, or re-execute the migration (as required above in R1), or migrate
 * to a new SN. The latter will probably still require some degree of user
 * intervention, by running the RepairPlan.
 *
 * - It was an implementation goal to be able to use the RelocateRN task, which
 * is used by elasticity plans, to move RNs from one SN to another. That would
 * reduce duplicate code, but more importantly the RelocateRNTask is more
 * atomic and more robust. This turned out to be non-trivial, because the
 * RelocateRN task assumes the old SN is up, while in this migrate-sn
 * situation, the old SN should be down, as the migrate-sn plan is supposed to
 * be used when the hardware is down. This needs investigation in the future.
 *
 * Migrating an SN between datacenters is not permitted.
 */
public class MigrateSNPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;
    private AdminId foundAdminId;
    private final StorageNodeId oldNode;
    private final StorageNodeId newNode;
    /*
     * httpPort is no longer used, but it is kept here
     * for serialization compatibility.
     */
    @SuppressWarnings("unused")
    @Deprecated
    private int httpPort;

    /**
     * Whether the plan was executed with the force flag. Added in 19.3.
     *
     * Note that calling 'plan migrate-sn -force' when one or more admins are
     * running a version earlier than 19.3 may cause the effects of specifying
     * -force to be lost. Because the force flag is being used to handle cases
     * where SNs are offline, there is no easy way to enforce that all nodes
     * have been upgraded to 19.3, so don't worry about this upgrade case. It
     * is OK that -force will only work dependably if all admins have been
     * upgraded.
     */
    private volatile boolean isForce;

    /**
     * Constructs a plan for replacing a node in the store.  Any resources
     * known to be allocated to the old node will be moved to the new node.
     *
     * @param oldNode the node getting replaced
     * @param newNode the node to replace the old node
     */
    public MigrateSNPlan(String name,
                         Planner planner,
                         Topology topology,
                         StorageNodeId oldNode,
                         StorageNodeId newNode) {

        super(name, planner, topology);

        this.oldNode = oldNode;
        this.newNode = newNode;

        /*
         * Check the store for correctness before attempting the migration.
         * Hope to stave off compounding failures by fixing/checking before a
         * new migration.
         */
        addTask(new VerifyBeforeMigrate(this, oldNode, newNode));

        /*
         * Check that the nodes exist in the admin metadata, and that services
         * are distributed as expected.
         */
        validate(topology, oldNode, newNode);

        /* Confirm that the new node is alive. */
        addTask(new ConfirmSNStatus(this,
                                    newNode,
                                    true /* shouldBeRunning*/,
                                    "Please ensure that " + newNode +
                                    " is deployed and running before " +
                                    "attempting a node migration."));

        /* Confirm that the old node is dead. */
        addTask(new ConfirmSNStatus(this,
                                    oldNode,
                                    false /* shouldBeRunning*/,
                                    "Please ensure that " + oldNode +
                                    " is stopped before attempting a node " +
                                    "migration."));

        /*
         * Update params and topology so all services on the old node are moved
         * to the new node,and push topology changes to all nodes.
         */
        addTask(new MigrateParamsAndTopo(this, oldNode, newNode));

        /* Update the membership address held in the target rep groups.*/
        addTask(new ChangeServiceAddresses(this, oldNode, newNode));

        /* Create the Admin service, if needed */
        if (foundAdminId != null) {
            addTask(new DeployAdmin(this, newNode, foundAdminId));
            // TODO: May need to check admin state after deployment
        }

        /* Create the RepNode services */
        addTask(new DeployMultipleRNs(this, newNode));

        /*
         * Add a set of tasks at the end to rewrite the peers of the target SNs
         * repNodeParams with the full set of helper hosts for the group, to
         * improve the robustness of the group when nodes fail. No need to
         * bounce the RepNodes -- helper hosts are mutable.
         */
        Set<RepGroup> affectedRGs = new HashSet<RepGroup>();
        for (RepGroup rg: topology.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                if (rn.getStorageNodeId().equals(oldNode)) {
                    affectedRGs.add(rg);
                    break;
                }
            }
            for (ArbNode an : rg.getArbNodes()) {
                if (an.getStorageNodeId().equals(oldNode)) {
                    affectedRGs.add(rg);
                    break;
                }
            }
        }

        for (RepGroup rg : affectedRGs) {
            for (RepNode rn : rg.getRepNodes()) {
                if (!rn.getStorageNodeId().equals(oldNode)) {
                    addTask(new UpdateHelperHostV2(this,
                                                   rn.getResourceId(),
                                                   rg.getResourceId()));
                    addTask(new NewRepNodeParameters(this,
                                                     rn.getResourceId()));
                }
            }
            for (ArbNode an : rg.getArbNodes()) {
                if (!an.getStorageNodeId().equals(oldNode)) {
                    addTask(new UpdateHelperHostV2(this,
                                                   an.getResourceId(),
                                                   rg.getResourceId()));
                    addTask(new NewArbNodeParameters(this,
                                                     an.getResourceId()));
                }

            }
        }

        if (foundAdminId != null) {
            /* Update the other Admins' helper host addresses. */
            addTask(new UpdateAdminHelperHost(this));
        }
    }

    private void validate(Topology topology,
                          StorageNodeId oldNode1,
                          StorageNodeId newNode1) {

        Parameters parameters = planner.getAdmin().getCurrentParameters();

        /* Confirm that the old node exists in the params and topology. */
        if (topology.get(oldNode1) == null) {
            throw new IllegalCommandException
                (oldNode1 + " does not exist in the topology and cannot " +
                 "be migrated");
        }

        if (parameters.get(oldNode1) == null) {
            throw new IllegalCommandException
                (oldNode1 + " does not exist in the parameters and cannot " +
                 "be migrated");
        }

        /* Confirm that the new node exists in the params and topology. */
        if (topology.get(newNode1) == null) {
            throw new IllegalCommandException
                (newNode1 + " is not in the topology and must be deployed " +
                 "before migrating " + oldNode1);
        }

        if (parameters.get(newNode1) == null) {
            throw new IllegalCommandException
                (newNode1 + " is not in the parameters and must be deployed " +
                 "before migrating " + oldNode1);
        }

        /*
         * Services should not exist in the topology and parameters for both
         * the new node and the old node.
         *
         * If the plan has never been run, there should be no services on the
         * new node. If the plan has already been run, all services should have
         * been moved to the new node, and there should be nothing on the old
         * node, according to the topology and params.
         *
         * We permit running the plan though, in case the plan had
         * not completed successfully. It may be that the metadata was
         * changed and committed, but the actual deploy actions had not taken
         * effect.
         */

        boolean oldNodeAdminsExist = false;
        boolean newNodeAdminsExist = false;
        boolean oldNodeRNsExist = false;
        boolean newNodeRNsExist = false;
        boolean oldNodeANsExist = false;
        boolean newNodeANsExist = false;

        for (AdminParams ap: parameters.getAdminParams()) {
            if (ap.getStorageNodeId().equals(oldNode1)) {
                oldNodeAdminsExist = true;
                foundAdminId = ap.getAdminId();
            }

            if (ap.getStorageNodeId().equals(newNode1)) {
                newNodeAdminsExist = true;
                foundAdminId = ap.getAdminId();
            }
        }

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(oldNode1)) {
                oldNodeRNsExist = true;
            }

            if (rnp.getStorageNodeId().equals(newNode1)) {
                newNodeRNsExist = true;
            }
        }

        for (ArbNodeParams anp: parameters.getArbNodeParams()) {
            if (anp.getStorageNodeId().equals(oldNode1)) {
                oldNodeANsExist = true;
            }

            if (anp.getStorageNodeId().equals(newNode1)) {
                newNodeANsExist = true;
            }
        }


        if (oldNodeRNsExist && newNodeRNsExist) {
            throw new IllegalCommandException
                ("Cannot migrate services from " + oldNode1 + " to " +
                 newNode1 + " because " + newNode1 + " is already in use");
        }

        if (oldNodeAdminsExist && newNodeAdminsExist) {
            throw new IllegalCommandException
                ("Cannot migrate services from " + oldNode1 + " to " +
                 newNode1 + " because " + newNode1 + " is already in use");
        }


        if (oldNodeANsExist && newNodeANsExist) {
            throw new IllegalCommandException
                ("Cannot migrate services from " + oldNode1 + " to " +
                 newNode1 + " because " + newNode1 + " is already in use");
        }

        if ((!oldNodeAdminsExist) &&
            (!oldNodeRNsExist) &&
            (!oldNodeANsExist) &&
            (!newNodeAdminsExist) &&
            (!newNodeRNsExist) &&
            (!newNodeANsExist)) {
            throw new IllegalCommandException
                ("No services on " + oldNode1 + " or " + newNode1 +
                 ", nothing to migrate");
        }

        final Datacenter oldDC = topology.getDatacenter(oldNode1);
        final Datacenter newDC = topology.getDatacenter(newNode1);
        if (!oldDC.equals(newDC)) {
            throw new IllegalCommandException(
                "Cannot migrate services from " + oldNode1 + " to " +
                newNode1 + " because they belong to different zones." +
                " Node " + oldNode1 + " belongs to zone: " + oldDC +
                ". Node " + newNode1 + " belongs to zone: " + newDC +
                ".");
        }
    }

    public StorageNodeId getOldNode() {
        return oldNode;
    }

    public StorageNodeId getNewNode() {
        return newNode;
    }

    /** Return whether this plan was executed with force flag */
    public boolean isForce() {
        return isForce;
    }

    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        /* Keep track of the plan's force flag */
        isForce = force;
        /* check that topology is the one when the plan was created.*/
        super.preExecuteCheck(force, executeLogger);
    }

    @Override
    public String getDefaultName() {
        return "Migrate Storage Node";
    }

}
