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

package oracle.kv.impl.topo.split;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.impl.rep.admin.ClientRepNodeAdminAPI;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * An object that represents a portion of a topology which can be operated on
 * in parallel with other splits. A split consists of one or more sets of
 * partitions. In order to minimize contention with other processing the
 * sets of partitions should be processed in order. The partitions within a
 * set can be processed in parallel with each other.
 */
public class TopoSplit {

    /**
     * This class is only used by utilities, so OK to use a standard logger.
     */
    private static final Logger logger =
        Logger.getLogger(TopoSplit.class.getName());

    /* Split id, for debugging */
    private final int id;

    /* Lists of partition sets */
    private final List<Set<Integer>> partitionSets;

    TopoSplit(int id) {
        this.id = id;
        partitionSets = new ArrayList<Set<Integer>>();
    }

    public TopoSplit(int id, Set<Integer> partitionSet) {
        this(id);
        partitionSets.add(partitionSet);
    }

    /**
     * Returns true if there are no partition sets in this split, otherwise
     * false.
     *
     * @return true if there are no partition sets in this split
     */
    public boolean isEmpty() {
        return partitionSets.isEmpty();
    }

    int getId() {
        return id;
    }

    void add(Set<Integer> set) {
        partitionSets.add(set);
    }

    void addAll(List<Set<Integer>> pSets) {
        partitionSets.addAll(pSets);
    }

    int size() {
        return partitionSets.size();
    }

    /**
     * Gets the list of partition sets in this split.
     *
     * @return the list of partition sets
     */
    public List<Set<Integer>> getPartitionSets() {
        return partitionSets;
    }

    Set<Integer> getPartitionSet(int slot) {
        return (slot >= partitionSets.size()) ? null : partitionSets.get(slot);
    }

    /**
     * Gets the set of storage nodes which house the partitions in this split.
     * The set is filtered by the specified consistency.
     *
     * @return the set of storage nodes which house the partitions in this split
     */
    @SuppressWarnings("deprecation")
    public Set<StorageNode> getSns(Consistency consistency,
                                   Topology topology,
                                   RegistryUtils regUtils) {

        final Set<StorageNode> sns = new HashSet<StorageNode>();
        final Set<StorageNode> snsChecked = new HashSet<>();
        final Set<RepNode> rnsChecked = new HashSet<>();
        for (Set<Integer> set : partitionSets) {
            for (Integer i : set) {
                final PartitionId partId = new PartitionId(i);
                final RepGroupId repGroupId = topology.getRepGroupId(partId);

                if (repGroupId == null) {
                    throw new IllegalArgumentException("Topology has not been" +
                                                       " initialized");
                }
                final RepGroup repGroup = topology.get(repGroupId);
                final Collection<RepNode> repNodes = repGroup.getRepNodes();

                for (RepNode rn : repNodes) {
                    if (!rnsChecked.add(rn)) {
                        continue;
                    }
                    final StorageNodeId snid = rn.getStorageNodeId();
                    final StorageNode sn = topology.get(snid);
                    if (!snsChecked.add(sn)) {
                        continue;
                    }
                    ReplicatedEnvironment.State state = null;
                    try {
                        final ClientRepNodeAdminAPI rna =
                            regUtils.getClientRepNodeAdmin(
                                rn.getResourceId(), Protocols.getDefault());
                        state = rna.getReplicationState();
                    } catch (RemoteException re) {
                        logger.info("Getting replication state failed for " +
                                    rn.getResourceId() + ": " +
                                    re.getMessage());
                    } catch (NotBoundException e) {
                        logger.info("No admin service for RN: " +
                                    rn.getResourceId() +
                                    " message: " + e.getMessage());
                    }

                    if (state == null) {
                        continue;
                    }

                    if (!state.isActive() ||
                        (consistency == Consistency.NONE_REQUIRED_NO_MASTER &&
                         state.isMaster()) ||
                        (consistency == Consistency.ABSOLUTE &&
                         !state.isMaster())) {
                        continue;
                    }

                    sns.add(sn);
                }
            }
        }
        return sns;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TopoSplit[");
        sb.append(id);
        sb.append(", ");
        for (Set<Integer> set : partitionSets) {
            sb.append("\n\t");
            sb.append(set);
        }
        sb.append("]");
        return sb.toString();
    }
}
