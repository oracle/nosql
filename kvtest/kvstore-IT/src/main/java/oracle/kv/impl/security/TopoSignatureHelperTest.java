/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

public class TopoSignatureHelperTest extends TestBase {

    private static final File secDir =
        new File(TestUtils.getTestDir(), FileNames.SECURITY_CONFIG_DIR);

    private static SecurityParams sp;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        TestUtils.clearTestDirectory();
        TestUtils.generateSecurityDir(secDir);

        sp = ConfigUtils.getSecurityParams(
            new File(secDir, FileNames.SECURITY_CONFIG_FILE));
    }

    @Test
    public void testCreation() {
        /* Null security prarams */
        try {
            TopoSignatureHelper.buildFromSecurityParams(null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

       /* Unrecognized signature algorithm */
        try {
            sp.setSignatureAlgorithm("Unknown");
            TopoSignatureHelper.buildFromSecurityParams(sp);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */ finally {
            sp.setSignatureAlgorithm(null);
        }

        /* Invalid keystore password source */
        final String origKeystorePwdFile = sp.getPasswordFile();
        try {
            sp.setPasswordFile(null);
            TopoSignatureHelper.buildFromSecurityParams(sp);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */ finally {
            sp.setPasswordFile(origKeystorePwdFile);
        }

        /* Invalid keystore file */
        final String origKeystoreFile = sp.getKeystoreFile();
        try {
            sp.setKeystoreFile("Unknown");
            TopoSignatureHelper.buildFromSecurityParams(sp);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */ finally {
            sp.setKeystoreFile(origKeystoreFile);
        }

        /* Invalid certstore file */
        final String origCertStoreFile = sp.getTruststoreFile();
        try {
            sp.setTruststoreFile("Unknown");
            TopoSignatureHelper.buildFromSecurityParams(sp);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */ finally {
            sp.setTruststoreFile(origCertStoreFile);
        }
    }

    @Test
    public void testTopoBytesForSignature() throws IOException {
        final Topology t1 = TopoUtils.create("testStore", 1 /* nDC */,
                                             3 /* nSN */, 3 /*repFactor */,
                                             100 /* nPartitions */);
        final byte[] t1BytesForSig = t1.toByteArrayForSignature();

        /* Copies of topo should have same byte arrays for signature */
        final Topology t1Copy = t1.getCopy();
        byte[] t1CopyBytesForSig = t1Copy.toByteArrayForSignature();
        assertTrue(Arrays.equals(t1BytesForSig, t1CopyBytesForSig));

        /* Evolve t1 copy, byte arrays for signature should have changed */
        final TopoEvolution topoEvolutions[] =
            new TopoEvolution[] {
                new AddZone(),
                new RemoveZone(),
                new AddStorageNode(),
                new RemoveStorageNode(),
                new AddRepGroup(),
                new RemoveRepGroup(),
                new AddRepNode(),
                new RemoveRepNode(),
                new AddPartition(),
                new ChangeSeqNumber()
        };

        for (TopoEvolution topoEvolve : topoEvolutions) {
            final Topology anotherT1Copy = t1.getCopy();
            topoEvolve.evolve(anotherT1Copy);
            t1CopyBytesForSig = anotherT1Copy.toByteArrayForSignature();
            assertFalse(Arrays.equals(t1BytesForSig, t1CopyBytesForSig));
        }

        /*
         * Two topologies that have the same layout but different object
         * references previously would produce different serialized byte array.
         * Fixed in [#25861].
         */
        final Topology t2 = new Topology("testStore", 0);
        final Topology t3 = new Topology("testStore", 0);

        /* Two DatacenterId instances with the same id */
        final Datacenter dc1 =
            Datacenter.newInstance("dc", 1, DatacenterType.PRIMARY, false,
                                   false);
        dc1.setResourceId(new DatacenterId(1));
        final Datacenter dc1Copy =
            Datacenter.newInstance("dc", 1, DatacenterType.PRIMARY, false,
                                   false);
        dc1Copy.setResourceId(new DatacenterId(1));

        final StorageNode sn1 = new StorageNode(dc1, "host1", 5000);
        final StorageNode sn2 = new StorageNode(dc1, "host2", 5000);
        final StorageNode sn1Copy = new StorageNode(dc1, "host1", 5000);
        final StorageNode sn2Copy = new StorageNode(dc1Copy, "host2", 5000);

        t2.add(sn1);
        t2.add(sn2);
        t3.add(sn1Copy);
        t3.add(sn2Copy);
        final byte[] t2BytesForSig = t2.toByteArrayForSignature();
        final byte[] t3BytesForSig = t3.toByteArrayForSignature();
        assertTrue(Arrays.equals(t2BytesForSig, t3BytesForSig));

        /* Two topologies that have different topology ID would produce
         * different serialized byte array.*/
        final Topology t4 = new Topology("testStore", 0);
        final Topology t5 = new Topology("testStore", 1);

        final byte[] t4BytesForSig = t4.toByteArrayForSignature();
        final byte[] t5BytesForSig = t5.toByteArrayForSignature();
        assertFalse(Arrays.equals(t4BytesForSig, t5BytesForSig));
    }

    @Test
    public void testSignAndVerify() {

        final TopoSignatureHelper topoSigHelper =
            TopoSignatureHelper.buildFromSecurityParams(sp);

        final Topology t1 = TopoUtils.create("testStore", 1 /* nDC */,
                                             3 /* nSN */, 3 /*repFactor */,
                                             100 /* nPartitions */);

        /* Basic sign-verify test */
        final byte[] t1Sig = topoSigHelper.sign(t1);
        assertNotNull(t1Sig);
        assertTrue(topoSigHelper.verify(t1, t1Sig));

        /* Modified signature should fail verification */
        final byte[] modifiedT1Sig = Arrays.copyOf(t1Sig, t1Sig.length);
        modifiedT1Sig[0] = (byte) (modifiedT1Sig[0] + 1);
        assertFalse(topoSigHelper.verify(t1, modifiedT1Sig));

        /* Identical topologies should have same signatures */
        final Topology t1Copy = t1.getCopy();
        final byte[] t1CopySig = topoSigHelper.sign(t1Copy);
        assertTrue(Arrays.equals(t1Sig, t1CopySig));
        assertTrue(topoSigHelper.verify(t1, t1CopySig));

        /* Evolve t1 */
        arbitrarilyEvolveTopology(t1);

        /* Evolved topology should fail verification with old signature */
        assertFalse(topoSigHelper.verify(t1, t1Sig));

        final byte[] evolvedT1Sig = topoSigHelper.sign(t1);

        /*
         * Topology updated with changes should pass verification with original
         * source topo's signature
         */
        final TopologyInfo topoChangeInfo =
            t1.getChangeInfo(t1Copy.getSequenceNumber() + 1);
        t1Copy.apply(topoChangeInfo.getChanges());
        assertTrue(topoSigHelper.verify(t1Copy, evolvedT1Sig));

        /* History topo changes not should affect verification */
        t1Copy.pruneChanges(t1Copy.getSequenceNumber() - 1, Integer.MAX_VALUE);
        assertTrue(topoSigHelper.verify(t1Copy, evolvedT1Sig));
    }

    /* Arbitrarily modify a topology via evolution */
    private void arbitrarilyEvolveTopology(Topology topo) {
        final Datacenter dc2 =
            Datacenter.newInstance("dc2", 3 /* rf */,
                                   DatacenterType.PRIMARY, false, false);
        topo.add(dc2);
        topo.add(new StorageNode(dc2, "local", 5000));
        topo.add(new RepGroup());
    }

    private static interface TopoEvolution {
        void evolve(Topology topo);
    }

    static class AddZone implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            final Datacenter newDc =
                Datacenter.newInstance("dc2", 3 /* rf */,
                                       DatacenterType.PRIMARY, false, false);
            topo.add(newDc);
        }
    }

    static class RemoveZone implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            final Datacenter dcs[] =
                topo.getSortedDatacenters().toArray(new Datacenter[0]);
            if (dcs != null && dcs.length != 0) {
                topo.remove(dcs[0].getResourceId());
            }
        }
    }

    static class AddStorageNode implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            final Datacenter dcs[] =
                topo.getSortedDatacenters().toArray(new Datacenter[0]);
            for (Datacenter dc : dcs) {
                topo.add(new StorageNode(dc, "local", 5000));
                return;
            }
        }
    }

    static class RemoveStorageNode implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            for (StorageNodeId snid : topo.getStorageNodeIds()) {
                topo.remove(snid);
                return;
            }
        }
    }

    static class AddRepGroup implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            topo.add(new RepGroup());
        }
    }

    static class RemoveRepGroup implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            for (RepGroupId rgid : topo.getRepGroupIds()) {
                topo.remove(rgid);
                return;
            }
        }
    }

    static class AddRepNode implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            final List<StorageNode> sns = topo.getSortedStorageNodes();
            if (sns != null && !sns.isEmpty()) {
                for (RepGroupId rgid : topo.getRepGroupIds()) {
                    topo.get(rgid).add(
                        new RepNode(sns.get(0).getStorageNodeId()));
                    return;
                }
            }
        }
    }

    static class RemoveRepNode implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            for (RepNodeId rnid : topo.getRepNodeIds()) {
                topo.remove(rnid);
                return;
            }
        }
    }

    static class AddPartition implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            for (RepGroupId rgid : topo.getRepGroupIds()) {
                topo.add(new Partition(rgid));
            }
        }
    }

    static class ChangeSeqNumber implements TopoEvolution {

        @Override
        public void evolve(Topology topo) {
            /*
             * Change the topology seq number by adding and then removing a
             * datacenter
             */
            topo.add(Datacenter.newInstance("dc2", 3 /* rf */,
                                       DatacenterType.PRIMARY, false, false));
            topo.remove(new DatacenterId(2));
        }
    }
}
