/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna.masterBalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.logging.Logger;

import oracle.kv.TestBase;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MasterLeaseInfo;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.PollCondition;

import org.junit.Test;

/**
 * Unit tests for the lease manager used during master balancing
 */
public class MasterLeaseManagerTest extends TestBase {

    final RepNodeId rn11Id = new RepNodeId(1, 1);
    final RepNodeId rn12Id = new RepNodeId(1, 2);
    final StorageNodeId sn1Id = new StorageNodeId(1);
    final StorageNodeId sn2Id = new StorageNodeId(2);

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    @Test
    public void testBasic() {
        final MasterLeaseManager lm =
                new MasterLeaseManager(Logger.getLogger("utlogger"));

        final Topology topo = TopoUtils.create("burl", 1, 3, 3, 100);

        StorageNode sn1 = topo.get(sn1Id);
        RepNode rn11 = topo.get(rn11Id);

        MasterLeaseInfo l1 = new MasterLeaseInfo(sn1, rn11, 100, 10000);
        assertTrue(lm.getMasterLease(l1));
        assertEquals(1, lm.leaseCount());

        MasterLeaseInfo l2 =
                new MasterLeaseInfo(sn1, topo.get(rn12Id), 100, 10000);
        assertTrue(lm.getMasterLease(l2));
        assertEquals(2, lm.leaseCount());

        assertTrue(lm.cancel(sn1, rn11));
        assertEquals(1, lm.leaseCount());

        /* reject cancel attempt when lessee is not the rightful owner */
        assertFalse(lm.cancel(topo.get(sn2Id), rn11));

        /* test lease replacement: replace the lease with a shorter one. */
        l2 = new MasterLeaseInfo(sn1, topo.get(rn12Id), 100, 2000);
        assertTrue(lm.getMasterLease(l2));
        assertEquals(1, lm.leaseCount());

        /* Test for lease expiration. */
        assertTrue(new PollCondition(100, 10000) {

            @Override
            protected boolean condition() {
                return lm.leaseCount() == 0;
            }

        }.await());

        /* Lessee-independent lease cancellation. */
        assertTrue(lm.getMasterLease(l1));
        assertTrue(lm.cancel(l1.rn.getResourceId()));
        assertEquals(0, lm.leaseCount());

        lm.shutdown();
    }
}
