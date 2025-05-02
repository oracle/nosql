/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.util;

import static org.junit.Assert.assertEquals;
import oracle.kv.TestBase;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

public class KerberosPrincipalsTest extends TestBase {

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void testBasic()
        throws Exception {

        SNKrbInstance[] ins1 =
            new SNKrbInstance[] { new SNKrbInstance("node1.example.com", 1),
                                  new SNKrbInstance("node2.example.com", 2),
                                  new SNKrbInstance("node3.example.com", 3)
        };

        /* Make an artificial topology */
        Topology topo = new Topology("Topo");
        Datacenter dc1 = topo.add(
            Datacenter.newInstance("EC-datacenter", 2,
                                   DatacenterType.PRIMARY, false, false));
        StorageNode sn1 = topo.add(new StorageNode(dc1,"node1", 5000));
        StorageNode sn2 = topo.add(new StorageNode(dc1,"node2", 5000));
        StorageNode sn3 = topo.add(new StorageNode(dc1,"node3", 5000));

        KerberosPrincipals princs1 = new KerberosPrincipals(ins1);
        assertEquals(princs1.getSNInstanceNames().length, 3);
        assertEquals(ins1[0].getInstanceName(),
                     princs1.getInstanceName(sn1));
        assertEquals(ins1[1].getInstanceName(),
                     princs1.getInstanceName(sn2));
        assertEquals(ins1[2].getInstanceName(),
                     princs1.getInstanceName(sn3));

        /* serialize it */
        final KerberosPrincipals princs2 = TestUtils.serialize(princs1);
        final SNKrbInstance[] ins2 = princs2.getSNInstanceNames();

        assertEquals(ins2.length, 3);
        assertEquals(ins2[0].getInstanceName(),
                     ins1[0].getInstanceName());
        assertEquals(ins2[1].getInstanceName(),
                     ins1[1].getInstanceName());
        assertEquals(ins2[2].getInstanceName(),
                     ins1[2].getInstanceName());

        assertEquals(princs2.getInstanceName(sn1), ins1[0].getInstanceName());
        assertEquals(princs2.getInstanceName(sn2), ins1[1].getInstanceName());
        assertEquals(princs2.getInstanceName(sn3), ins1[2].getInstanceName());

        /* fast serialize it */
        final KerberosPrincipals princs3 = TestUtils.fastSerialize(princs1);
        final SNKrbInstance[] ins3 = princs3.getSNInstanceNames();

        assertEquals(ins3.length, 3);
        assertEquals(ins3[0].getInstanceName(),
                     ins1[0].getInstanceName());
        assertEquals(ins3[1].getInstanceName(),
                     ins1[1].getInstanceName());
        assertEquals(ins3[2].getInstanceName(),
                     ins1[2].getInstanceName());

        assertEquals(princs3.getInstanceName(sn1), ins1[0].getInstanceName());
        assertEquals(princs3.getInstanceName(sn2), ins1[1].getInstanceName());
        assertEquals(princs3.getInstanceName(sn3), ins1[2].getInstanceName());
    }
}
