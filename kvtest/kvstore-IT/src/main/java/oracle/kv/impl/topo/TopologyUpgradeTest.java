/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.topo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.fault.UnknownVersionException;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Verifies that an R1 topology is upgraded as expected.
 */
public class TopologyUpgradeTest extends TestBase {

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
       Topology topo = populateTopology(100 /* shards */, /* RF */ 2, 3);

       /* No upgrades for a topology that's current. */
       assertFalse(topo.upgrade());

       topo.setVersion(Integer.MAX_VALUE);
       try {
           topo.upgrade();
           fail("expected UVE");
       } catch (UnknownVersionException uve) {
           /* expected. */
       }

       final HashMap<Datacenter, Integer> rfMap =
           new HashMap<Datacenter, Integer>();

       /* Simulate a r1 topology with RF = 0 */
       for (Datacenter dc : topo.getDatacenterMap().getAll()) {
           rfMap.put(dc, dc.getRepFactor());
           dc.setRepFactor(0);
       }

       topo.setVersion(0); /* Make the topo r1 */
       /* Verify that topology is upgraded when RF = 0 */
       assertTrue(topo.upgrade());

       for (Datacenter dc : topo.getDatacenterMap().getAll()) {
           assertEquals(dc.getRepFactor(), rfMap.get(dc).intValue());
       }

       /* No need to upgrade it again. */
       assertFalse(topo.upgrade());

       /* Create a bad r1 topology where some DCs RF is being violated. */

       topo.setVersion(0); /* Make the topo r1 */
       /* Zero out the RF, to simulate R1 */
       for (Datacenter dc : topo.getDatacenterMap().getAll()) {
           dc.setRepFactor(0);
       }

       /* Associate a random sn with some other DC */
       StorageNode sn1 = topo.getStorageNodeMap().getAll().iterator().next();
       DatacenterId dcId = sn1.getDatacenterId();
       StorageNode sn2 = null;
       for (Datacenter dc : topo.getDatacenterMap().getAll()) {
           if (!dcId.equals(dc.getResourceId())) {
               sn2 = new StorageNode(dc, "bad_dc",
                                     TestUtils.DUMMY_REGISTRY_PORT);
           }
       }
       topo.update(sn1.getResourceId(), sn2);

       /*
        * have our bad topology, check for exception
        */
       assertFalse(topo.upgrade());
       assertEquals(0, topo.getVersion());

       /* Verify upgrade on an empty Topology. An admin can have one. */
       final Topology nullTopo = new Topology("t");
       nullTopo.setVersion(0); /* Make the null topo r1 */
       assertTrue(nullTopo.upgrade());
       assertTrue(nullTopo.getVersion() > 0);
    }

    /*
     * Populate a Topology with the number of DCs determined by the
     * number of rfs.
     */
    private Topology populateTopology(int numShards, int... rfs) {
        Topology topo = new Topology("t");

        addDCs(topo, rfs);

        HashMap<Datacenter, Iterator<StorageNode>> dcSNMap =
            new HashMap<Datacenter, Iterator<StorageNode>>();

        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            dcSNMap.put(dc, addSNs(topo,
                                   dc,
                                   numShards * dc.getRepFactor()).iterator());
        }

        addShards(topo, dcSNMap, numShards);
        return topo;
    }

    private void addShards(Topology topo,
                           HashMap<Datacenter, Iterator<StorageNode>> dcSNMap,
                           int numShards) {

        for (int i = 0; i < numShards; i++) {
            final RepGroup rg = new RepGroup();
            topo.add(rg);
            for (Datacenter dc : topo.getDatacenterMap().getAll()) {
                for (int rnId = 0; rnId < dc.getRepFactor(); rnId++) {
                    rg.add(new RepNode(dcSNMap.get(dc).next().
                                       getStorageNodeId()));
                }
            }
        }
    }

    private void addDCs(Topology topo, int... rfs) {
        int dcId = 0;
        for (Integer rf : rfs) {
            topo.add(Datacenter.newInstance("dc" + ++dcId, rf,
                                            DatacenterType.PRIMARY, false,
                                            false));
        }
    }

    private List<StorageNode> addSNs(Topology topo,
                                     Datacenter dc,
                                     int numSNs) {
        final List<StorageNode> sns = new LinkedList<StorageNode>();
        for (int snId=1; snId <= numSNs; snId++) {
            final StorageNode sn =
                new StorageNode(dc, "hostname" + snId,
                                TestUtils.DUMMY_REGISTRY_PORT);
            sns.add(sn);
            topo.add(sn);
        }
        return sns;
    }
}
