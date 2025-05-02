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

package oracle.kv.impl.topo;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.topo.ResourceId.ResourceType;

/**
 * @see #writeFastExternal FastExternalizable format
 */
public class PartitionMap extends
    ComponentMap<PartitionId, Partition> {

    private static final long serialVersionUID = 1L;

    transient HashKeyToPartitionMap keyToPartitionMap;

    public PartitionMap(Topology topology) {
        super(topology);
    }

    PartitionMap(Topology topology, DataInput in, short serialVersion)
        throws IOException {

        super(topology, in, serialVersion);
    }

    /**
     * Returns the number of partitions in the partition map.
     */
    public int getNPartitions() {
        return cmap.size();
    }

    /**
     * Returns the partition id for the environment that contains the
     * replicated partition database associated with the given key.
     *
     * @param keyBytes the key used to identify the partition.
     *
     * @return the partition id that contains the key.
     */
    PartitionId getPartitionId(byte[] keyBytes) {
        if ((keyToPartitionMap == null) ||
            (keyToPartitionMap.getNPartitions() != size())) {
            /* Initialize transient field on demand. */
            keyToPartitionMap = new HashKeyToPartitionMap(size());
        }
        return keyToPartitionMap.getPartitionId(keyBytes);
    }

    /**
     * Returns the rep group id for the environment that contains the
     * replicated partition database associated with the given partition. If
     * the partition is not present null is returned. This may be due to the
     * map not being initialized before this method is called.
     *
     * @param partitionId the partitionId.
     *
     * @return the id of the RepGroup that contains the partition or null
     */
    public RepGroupId getRepGroupId(PartitionId partitionId) {
        final Partition p = cmap.get(partitionId);
        return (p == null) ? null : p.getRepGroupId();
    }

    /**
     * Returns a set containg the ids of all the partitions hosted in a given
     * range of shards.
     */
    public Set<Integer> getPartitionsInShards(int minShard, int maxShard) {

        Set<Integer> parts = new TreeSet<Integer>();

        for (Partition p : cmap.values()) {
            int sid = p.getRepGroupId().getGroupId();
            if  (sid >= minShard && sid <= maxShard) {
                parts.add(p.getResourceId().getPartitionId());
            }
        }

        return parts;
    }

    public List<PartitionId> getPartitionsInShard(
        int shard,
        Set<Integer> excludePartitions) {

        ArrayList<PartitionId> pids = new ArrayList<PartitionId>();

        for (Partition p : cmap.values()) {
            int sid = p.getRepGroupId().getGroupId();
            int pid = p.getResourceId().getPartitionId();
            if  (sid == shard &&
                 (excludePartitions == null ||
                  !excludePartitions.contains(pid))) {
                pids.add(p.getResourceId());
            }
        }

        return pids;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    PartitionId nextId() {
        return new PartitionId(nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
        return ResourceType.PARTITION;
    }

    @Override
    Class<Partition> getComponentClass() {
        return Partition.class;
    }
}
