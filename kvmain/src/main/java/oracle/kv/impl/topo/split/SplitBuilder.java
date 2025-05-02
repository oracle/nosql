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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.Consistency;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

/**
 * Class for constructing groupings of partitions (splits) for use by components
 * that want to optimally iterate over the store in parallel. A set of splits
 * returned by one of the create methods of this class will contain entries for
 * every partition in the store. A consumer of this set must iterate over the
 * partitions contained in each split in order to iterate over the complete
 * store. The goal is to provide a path for multiple processes, each operating
 * on a single split from the same, to iterate over the store with minimal
 * contention on the rep nodes, either from itself (using multiple threads)
 * or from other processes.
 * <p>
 * The set of splits can be constructed in different ways to meet specific
 * needs. In general, each split contains one or more partition sets in a list.
 * The intent is that the user should iterate over one partition set at a time.
 * A set may contain one or more partitions which can be accessed in
 * parallel with limited contention.
 * <p>
 * There are two basic ways in which a set of splits can be created, a
 * "partition split" and a "shard split". These differ in how the partitions
 * are allocated to each split.
 * <p>
 * Partition splits, returned by createPartitionSplits(int, Consistency),
 * assign partitions to each split sequentially, based on simple division.
 * A partition split can be calculated by independent processes and yield
 * the exact set of splits and partition sets for a given store.
 * <p>
 * Shard splits, returned by createShardSplits(int, Consistency) and
 * createShardSplits(Consistency), assign partitions based on membership in
 * a shard. A shard split is dependent on a specific topology version and may
 * not produce the same results given different topology versions of the same
 * store.
 * <p>
 * In either case, split optimization is done at the time of creation.
 * If the topology or replication node mastership changes between when the
 * split is created and when it is used, some loss in efficiency may result.
 * <p>
 * Split optimization is highly dependent on the topology. For example, more
 * rep nodes will permit a higher level of concurrency without contention.
 * Also, if the number of splits is specified in the create methods
 * (vs. automatically generated) it may not be possible to create an
 * optimal set.
 * <p>
 * Note that an implicit assumption in all of this is that partitions of a
 * given store are roughly the same size, and take the same amount of time
 * to iterate over. Furthermore, the assignment of partitions to a set assumes
 * that the request dispatcher will distribute accesses evenly across the
 * nodes in a shard (as consistency requirements allow).
 */
public class SplitBuilder {
    
    /* The target number of concurrent streams accessing each RN */
    private static final int TARGET_STREAMS_PER_RN = 2;
    
    /*
     * The target number of concurrent streams from each split. This value is
     * a somewhat arbitrary minimum used unless the requested number of splits
     * results in a higher value
     */
    private static final int TARGET_STREAMS_PER_SPLIT = 3;
        
    private final Topology topo;
    private final int storeRf;
    
    /**
     * Constructs a split builder with the specified topology. All subsequent
     * calls to create methods will use that topology.
     * 
     * @param topology the topology to use for this builder
     */
    public SplitBuilder(Topology topology) {
        this.topo = topology;
        
        if (topo.getRepGroupMap().size() < 1) {
            throw new IllegalArgumentException("Number of shards in store is 0");
        }
        
        int totalRf = 0;
        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isPrimary()) {
                totalRf += dc.getRepFactor();
            }
        }
        if (totalRf == 0) {
            throw new IllegalArgumentException("Store replication factor is 0");
        }
        storeRf = totalRf;
    }
    
    /**
     * Creates a list of splits based on partitions. This method creates the
     * specified number of splits where the number of partitions in each split
     * is the total number divided by nSplits. An attempt is made to order the
     * partitions within the splits to minimize contention on the rep
     * nodes. This method is safe to use in independent processes given the
     * same base topology. This is because the same splits will be generated
     * independent of changes in the topology.
     * 
     * If the number of splits is greater then the number of partitions, some
     * splits may be empty (i.e. contain no partition sets).
     * 
     * Note: The above guarantee assumes that the number of partitions of
     * a store cannot change.
     * 
     * @param nSplits the number of splits to create
     * @param consistency the consistency used to access the store
     * @return a list of splits based on partitions
     */
    public List<TopoSplit> createPartitionSplits(int nSplits,
                                                 Consistency consistency) {
        if (nSplits < 1) {
            throw new IllegalArgumentException("nSplits must be > 0");
        }

        final List<TopoSplit> splits = new ArrayList<TopoSplit>(nSplits);
        for (int i = 1; i <= nSplits; i++) {
            splits.add(new TopoSplit(i));
        }
        
        final int nShards = topo.getRepGroupMap().size();
        
        /*
         * This assumes accesses to a shard are spread across the RNs
         * housing the shard's partitions.
         */
        final int streamsPerShard = 
                calcShardConcurrency(consistency) * TARGET_STREAMS_PER_RN;
        
        /*
         * totalStreams is the optimal number of streams hitting the store.
         */
        final int totalStreams = streamsPerShard * nShards;

        /*
         * Calc the streams per split, rounding up if necessary so that
         * totalStreams are created. Note that we don't go over totalStreams
         * which could result in the last split having less partitions in each
         * set.
         */
        int streamsPerSplit = totalStreams / nSplits;
        if ((totalStreams % nSplits) != 0) {
            streamsPerSplit += 1;
        }
        
        final int nPartitions = topo.getPartitionMap().getNPartitions();
        final int splitSize = (nPartitions < nSplits) ? 1 : nPartitions/nSplits;
        
        int start = 1;
        int end = splitSize;
        
        for (TopoSplit split : splits) {
             /*
              * The last split covers the remaining partitions. If nPartitions
              * does not divide evenly by nSplits, the the last split may have
              * fewer partitions, and the end point will be < splitSize
              */
            if (split.getId() == nSplits) {
                end = nPartitions;
            }
            
            final Map<RepGroupId, Set<Integer>> map =
                                            createPartitionMap(start, end);
            while (!map.isEmpty()) {
                final PartitionSelector selector =
                                    new PartitionSelector(map, streamsPerShard);
                
                final Set<Integer> pSet = selector.getNextSet(streamsPerSplit);
                
                if (pSet == null) {
                    break;
                }
                split.add(pSet);
            }
            
            /*
             * Adjust pointers, if start > nPartions then we are done. Any
             * remaining splits will be empty.
             */
            start = end+1;
            end += splitSize;
            
            if (start > nPartitions) {
                break;
            }
        }
        
        return splits;
    }
    
    /**
     * Creates a list of topology splits for the store. This method creates
     * the specified number of splits based on the specified consistency.
     * The ordering of partition sets within each split will be optimized
     * to reduce contention on the rep nodes assuming that each split
     * is processed in parallel, and the partition sets are processed in
     * order. This optimization assumes the size of each partition is
     * roughly the same.
     * 
     * TODO - the nSplits parameter has likely come from a Hadoop API which
     * says that the parameter is a hint. In that case it may be better to
     * produce the optimized set, independent of the number requested.?
     * 
     * @param nSplits the target number of splits to generate
     * @param consistency the consistency used to access the store
     * @return a list of topology splits
     */
    List<TopoSplit> createShardSplits(int nSplits, Consistency consistency) {
        if (nSplits < 1) {
            throw new IllegalArgumentException("nSplits must be > 0");
        }
        return createShardSplitsInternal(nSplits, consistency);
    }
    
    /**
     * Creates a list of topology splits for the store. The number of splits
     * returned is the maximum number of splits which can be processed in
     * parallel without contention at the storage node given the specified
     * consistency. The ordering of partition sets within each split will
     * be optimized to reduce contention on the storage nodes assuming that
     * each split is processed in parallel, and the partition sets are
     * processed in order. This optimization assumes the size of each
     * partition is roughly the same.
     * 
     * @param consistency the consistency used to access the store
     * @return a list of topology splits
     */
    public List<TopoSplit> createShardSplits(Consistency consistency) {
        return createShardSplitsInternal(Integer.MAX_VALUE, consistency);
    }
    
    private List<TopoSplit> createShardSplitsInternal(int targetSplits,
                                                      Consistency consistency) {
        final int nShards = topo.getRepGroupMap().size();
        assert nShards != 0;
        
        /*
         * This assumes accesses to a shard are spread across the RNs
         * housing the shard's partitions.
         */
        final int streamsPerShard =
                calcShardConcurrency(consistency) * TARGET_STREAMS_PER_RN;
        
        /*
         * totalStreams is the optimal number of streams hitting the store.
         */
        final int totalStreams = streamsPerShard * nShards;
        
        /*
         * Strawman calculation of the optimal number of splits. This assumes
         * each split will handle multiple streams. Make sure there is at least
         * one split.
         */
        final int optimalNSplits = (totalStreams > TARGET_STREAMS_PER_SPLIT) ?
                                       totalStreams / TARGET_STREAMS_PER_SPLIT :
                                       1;
        
        /*
         * If optimal is at or under the targert go with that, otherwise
         * use the target value.
         */
        final int nSplits = (optimalNSplits <= targetSplits) ? optimalNSplits :
                                                               targetSplits;
        final List<TopoSplit> splits = new ArrayList<TopoSplit>(nSplits);
        for (int i = 1; i <= nSplits; i++) {
            splits.add(new TopoSplit(i));
        }
        
        /*
         * Calc the streams per split, rounding up if necessary so that
         * totalStreams are created. Note that we don't go over totalStreams
         * which could result in the last split having less partitions in each
         * set.
         * TODO - Is is possible to spread the shorted sets around?
         */
        int streamsPerSplit = totalStreams / nSplits;
        if ((totalStreams % nSplits) != 0) {
            streamsPerSplit += 1;
        }
        
        final Map<RepGroupId, Set<Integer>> map =
                 createPartitionMap(1, topo.getPartitionMap().getNPartitions());
        
        /*
         * Loop though the splits, picking off streamsPerShard partitions from
         * each group as we go. This continues until all  partitions are
         * selected.
         */
        while (!map.isEmpty()) {
            final PartitionSelector selector =
                                    new PartitionSelector(map, streamsPerShard);
            
            /*
             * Note that one pass though the splits will consume up to
             * totalStreams partitions.
             */
            for (TopoSplit split : splits) {
                final Set<Integer> pSet = selector.getNextSet(streamsPerSplit);
                
                if (pSet == null) {
                    break;
                }
                split.add(pSet);
            }
            assert selector.getNextSet(streamsPerSplit) == null;
        }
        return splits;
    }
    
    /**
     * Create a map of rep group => set of partitions in that group for
     * the partitions between start and end.
     * 
     * @return a map of rep group to the set of partitions in that group
     */
    private Map<RepGroupId, Set<Integer>> createPartitionMap(int startPartition,
                                                             int endPartition) {
        final int nPartitions = 1+(endPartition-startPartition);
        assert startPartition > 0;
        assert nPartitions > 0;
        
        final Map<RepGroupId, Set<Integer>> map =
                                    new HashMap<RepGroupId, Set<Integer>>();
        for (int i = startPartition; i <= endPartition; i++) {
            final PartitionId partId = new PartitionId(i);
            final RepGroupId rgid = topo.getRepGroupId(partId);
            Set<Integer> parts = map.get(rgid);
            if (parts == null) {
                parts = new HashSet<Integer>();
                map.put(rgid, parts);
            }
            parts.add(i);
        }
        return map;
    }
    
    /**
     * Calculates the number of accesses which can be made from a single split
     * to a shard without contention. Basically this returns 1, RF-1, or RF
     * dependent on consistency requirements.
     */
    @SuppressWarnings("deprecation")
    private int calcShardConcurrency(Consistency consistency) {
        if (consistency == Consistency.ABSOLUTE) {
             return 1;
        } else if (consistency == Consistency.NONE_REQUIRED_NO_MASTER) {
            return storeRf - 1;
        }
        return storeRf;
    }
    
    /**
     * A selector which will generate sets of partitions from a map of
     * rep group => partitions in that group. As partitions are selected they
     * are removed from the sets in the map. Likewise when a group no longer
     * has any partitions it is removed from the map. Therfore a way to check
     * if the selection is complete is to call isEmpty() on the map.
     */
    private class PartitionSelector {
        
        /*
         * The maximum number of partitions selected from any given shard
         * during a single pass through the partitions.
         */
        final int maxSelectFromShard;

        /*  An iterator over the sets of partitions per shards. */
        private final Iterator<Set<Integer>> shardItr;
        
        /* The current partition iterator. */
        Iterator<Integer> partitionItr;
        
        /*
         * Running total of how many partitions have been selected from the
         * current shard.
         */
        int selected = 0;
        
        PartitionSelector(Map<RepGroupId, Set<Integer>> map,
                          int maxSelectFromShard) {
            this.maxSelectFromShard = maxSelectFromShard;
            shardItr = map.values().iterator();
            nextPartitionItr();
        }
        
        private boolean nextPartitionItr() {
            partitionItr = shardItr.hasNext() ? shardItr.next().iterator() :
                                                null;            
            return partitionItr != null;
        }
        
        /*
         * Gets the next partition. This will return the next partition in the
         * shard. Once maxSelectFromShard partitions have been taken
         * move on to the next shard. Return null if at the end of the
         * list.
         */
        private Integer getNextPartition() {

            /* If the iterator is null the set is done. */
            if (partitionItr == null) {
                return null;
            }
            
            /*  If we selected enough from this shard, move to the next. */
            if (selected >= maxSelectFromShard) {
                if (!nextPartitionItr()) {
                    return null;
                }
                selected = 0;
            }
            assert partitionItr != null;
            
            /*
             * If the next shard is empty, remove that shard and try again.
             */
            while (!partitionItr.hasNext()) {
                shardItr.remove();
                if (!nextPartitionItr()) {
                    return null;
                }
                assert partitionItr != null;
            }
            selected++;
            final Integer ret = partitionItr.next();
            partitionItr.remove();
            return ret;
        }
        
        /*
         * Gets the next set of setSize partitions. The set returned may
         * contain less than setSize entries. Returns null if at the
         * end of this pass.
         */
        Set<Integer> getNextSet(int setSize) {
            assert selected >= 0;
            
            Set<Integer> pSet = null;
            
            for (int i = 0; i < setSize; i++) {
                Integer p = getNextPartition();
                
                if (p == null) {
                    break;
                }
                if (pSet == null) {
                    pSet = new HashSet<Integer>();
                }
                pSet.add(p);
            }
            return pSet;
        }
    }
}
