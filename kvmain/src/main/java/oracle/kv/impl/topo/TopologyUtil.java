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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class for methods related to {@link Topology}.
 */
public class TopologyUtil {

    /**
     * Return a map of replication groups to partitions Ids. Groups with no
     * partitions will have empty values.
     */
    public static Map<RepGroupId, List<PartitionId>>
        getRGIdPartMap(Topology topology) {

        final Map<RepGroupId, List<PartitionId>> map = new HashMap<>();

        /*
         * Make sure that every rep group has an entry, even if it does not
         * contain any partitions
         */
        for (final RepGroupId rgId : topology.getRepGroupIds()) {
            map.put(rgId, new ArrayList<>());
        }
        for (Partition p : topology.getPartitionMap().getAll()) {
            map.get(p.getRepGroupId()).add(p.getResourceId());
        }

        return map;
    }

    /**
     * Returns the number of repNodes can be used for read operations.
     */
    public static int getNumRepNodesForRead(Topology topology,
                                            int[] readZoneIds) {
        final List<Integer> readZoneIdsLst;
        if (readZoneIds != null) {
            readZoneIdsLst = new ArrayList<>(readZoneIds.length);
            for (int id : readZoneIds) {
                readZoneIdsLst.add(id);
            }
        } else {
            readZoneIdsLst = null;
        }

        final Collection<Datacenter> datacenters =
            topology.getDatacenterMap().getAll();
        int num = 0;
        for (Datacenter dc: datacenters) {
            if (readZoneIdsLst != null) {
                final int dcId = dc.getResourceId().getDatacenterId();
                if (!readZoneIdsLst.contains(dcId)) {
                    continue;
                }
            }
            num += dc.getRepFactor();
        }
        final int nShards = topology.getRepGroupMap().size();
        return num * nShards;
    }

    /**
     * Returns all the rep groups in the topology ordered by their group ids.
     */
    public static List<RepGroup> getOrderedRepGroups(Topology topology) {
        final List<RepGroup> orderedGroups =
            new ArrayList<>(topology.getRepGroupMap().getAll());
        Collections.sort(orderedGroups,
                         (g1, g2) ->
                         g1.getResourceId().compareTo(g2.getResourceId()) );
        return orderedGroups;
    }
}
