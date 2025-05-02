/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.topo.split;

import java.util.List;
import java.util.Set;
import junit.framework.TestCase;
import oracle.kv.Consistency;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.TopoUtils;

/**
 *
 */
public class TestSplit extends TestCase {
    
    public void testCreatePartitionSplits() {
        final int NUM_PARTITIONS = 234;
        final Topology topo = TopoUtils.create("testStore",
                                               1, //DCs
                                               15, //SNs
                                               3, //RF
                                               NUM_PARTITIONS);
        final SplitBuilder sb = new SplitBuilder(topo);
        
        /* # splis < # partitions */
        
        List<TopoSplit> splits =
                    sb.createPartitionSplits(10, Consistency.NONE_REQUIRED);
        assertEquals(splits.size(), 10);
        
        checkSplits(splits, NUM_PARTITIONS);
        
        /* # splits == # partitions */
        splits = sb.createPartitionSplits(NUM_PARTITIONS,
                                          Consistency.NONE_REQUIRED);
        checkSplits(splits, NUM_PARTITIONS);
        
        /* # splits > # partitions */
        splits = sb.createPartitionSplits(NUM_PARTITIONS*2,
                                          Consistency.NONE_REQUIRED);
        checkSplits(splits, NUM_PARTITIONS);
    }

    @SuppressWarnings("deprecation")
    public void testCreateNShardSplits() {
        final int NUM_PARTITIONS = 123;
        final Topology topo = TopoUtils.create("testStore",
                                               1, //DCs
                                               20, //SNs
                                               3, //RF
                                               NUM_PARTITIONS);

        final SplitBuilder sb = new SplitBuilder(topo);
        List<TopoSplit> splits =
                        sb.createShardSplits(10, Consistency.NONE_REQUIRED);
        assertEquals(splits.size(), 10);
        
        checkSplits(splits, NUM_PARTITIONS);
        
        splits = sb.createShardSplits(NUM_PARTITIONS,
                                      Consistency.NONE_REQUIRED);
        
        checkSplits(splits, NUM_PARTITIONS);
        
        splits = sb.createShardSplits(NUM_PARTITIONS,
                                      Consistency.NONE_REQUIRED_NO_MASTER);
        
        checkSplits(splits, NUM_PARTITIONS);
        
        splits = sb.createShardSplits(NUM_PARTITIONS*2, Consistency.ABSOLUTE);
        
        checkSplits(splits, NUM_PARTITIONS);
    }
        
    public void testCreateShardSplits() {
        final int NUM_PARTITIONS = 789;
        final Topology topo = TopoUtils.create("testStore",
                                               1, //DCs
                                               20, //SNs
                                               3, //RF
                                               NUM_PARTITIONS);

        final SplitBuilder sb = new SplitBuilder(topo);
        List<TopoSplit> splits = sb.createShardSplits(Consistency.NONE_REQUIRED);
        
        checkSplits(splits, NUM_PARTITIONS);
        
        splits = sb.createShardSplits(Consistency.ABSOLUTE);
        
        checkSplits(splits, NUM_PARTITIONS);
    }
    
    /*
     * Checks that the list of splits contains all of the partitions and
     * that there is only one of each partition.
     */
    private void checkSplits(List<TopoSplit> splits, int nPartitions) {
        final boolean[] present = new boolean[nPartitions];
        
        for (TopoSplit split : splits) {
            for (Set<Integer> partitions : split.getPartitionSets()) {
                for (Integer p : partitions) {
                    assertFalse("duplicate found: " + p, present[p-1]);
                    present[p-1] = true;
                }
            }
        }
        // All should be accounted for
        for (int i = 0; i < nPartitions; i++) {
            assertTrue("Partition missing: " + (i+1), present[i]);
        }
    }
}
