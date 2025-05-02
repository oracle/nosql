/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.topo;

import static oracle.kv.util.TestUtils.checkException;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.TestUtils.fastSerialize;
import static oracle.kv.impl.util.TestUtils.serialize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.api.TopologyHistoryCache;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Tests Topology and its interfaces.
 *
 */
public class TopologyTest extends TestBase {

    final Topology topo = new Topology("TestStore", 1234567);

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * An illustrative test creating each element of the Topology
     */
    @Test
    public void testBasic()
        throws IOException, ClassNotFoundException {

        Datacenter dc1 = topo.add(
            Datacenter.newInstance("EC-datacenter", 2,
                                   DatacenterType.PRIMARY, false, false));
        Datacenter dc2 = topo.add(
            Datacenter.newInstance("WC-datacenter", 1,
                                   DatacenterType.SECONDARY, false, false));

        StorageNode sn1 =
            topo.add(new StorageNode(dc1,
                                     "sn1-hostname",
                                     TestUtils.DUMMY_REGISTRY_PORT));
        StorageNode sn2 = topo.add
            (new StorageNode(dc2,
                             "sn2-hostname",
                             TestUtils.DUMMY_REGISTRY_PORT));
        RepGroup rg1 = topo.add(new RepGroup());

        RepNode rn1 = rg1.add(new RepNode(sn1.getResourceId()));
        RepNode rn2 = rg1.add(new RepNode(sn2.getResourceId()));

        Partition p1 = topo.add(new Partition(rg1));
        Partition p2 = topo.add(new Partition(rg1));

        /* Verify that all the created elements are present in the topology */
        assertPresent(dc1, dc2, sn1, sn2, rg1, rn1, rn2, p1, p2);

        /* Basic change list verification. */
        assertEquals(9, topo.getSequenceNumber());
        assertEquals(9, topo.getChanges(1).size());

        /* Verify that they can be serialized. */
        verifySerialization(topo);

        serialVersionChecker(
            topo,
            SerialVersion.MINIMUM, 0x4ecf1f5fe820082bL)
            .equalsChecker((original, deserialized, sv) -> {
                    try {
                        assertTopoEquals(original, (Topology) deserialized,
                                         sv);
                        return true;
                    } catch (Throwable t) {
                        System.err.println("Not equals: " + t);
                        return false;
                    }
                })
            .check();

        Set<RepNodeId> ids = topo.getRepNodeIds();
        assertEquals(2, ids.size());
        assertTrue(ids.contains(rn1.getResourceId()));
        assertTrue(ids.contains(rn2.getResourceId()));

        /*
         * Ensure that the RepNodeId sort really works; the update of helper
         * hosts depends upon this.
         */
        List<RepNodeId> sRN = topo.getSortedRepNodeIds(rg1.getResourceId());
        assertEquals(2, sRN.size());
        assertEquals(rn1.getResourceId(), sRN.get(0));
        assertEquals(rn2.getResourceId(), sRN.get(1));

        assertEquals(Topology.EMPTY_SEQUENCE_NUMBER,
                     new Topology("foo").getSequenceNumber());
    }

    @Test
    public void testUpdateDelete()
        throws IOException, ClassNotFoundException {
        /* The topology that's incrementally updated. */
        Topology incTopo = new Topology(topo.getKVStoreName());
        Datacenter dc1 = topo.add(
            Datacenter.newInstance("EC-datacenter", 1,
                                   DatacenterType.PRIMARY, false, false));

        StorageNode sn1 = topo.add(new StorageNode(dc1,
                                                  "sn1-hostname",
                                                  Registry.REGISTRY_PORT));
        RepGroup rg1 = topo.add(new RepGroup());
        RepNode rn1 = rg1.add(new RepNode(sn1.getResourceId()));
        Partition p1 = topo.add(new Partition(rg1));

        /* Verify that add changes can be applied. */
        verifyApplyChangelist(topo, incTopo);

        Datacenter dc1u = topo.update(
            dc1.getResourceId(),
            Datacenter.newInstance("XEC-datacenter", 1,
                                   DatacenterType.PRIMARY, false, false));
        StorageNode sn1u = topo.update(sn1.getResourceId(),
                                       new StorageNode(dc1u,
                                                       "Xsn1-hostname",
                                                       Registry.REGISTRY_PORT));
        RepNode rn1u = rg1.update(rn1.getResourceId(),
                                  new RepNode(sn1u.getResourceId()));
        Partition p1u = topo.update(p1.getResourceId(), new Partition(rg1));

        /* Verify that all the updated elements are present in the topology */
        assertPresent(dc1u, sn1u, rg1, rn1u, p1u);

        /* Verify that all the pre-update elements are absent */
        assertReplaced(dc1, sn1, rn1,  p1);

        verifyApplyChangelist(topo, incTopo);

        /* test delete. Order matters */
        topo.remove(rn1u.getResourceId());
        topo.remove(p1u.getResourceId());
        topo.remove(rg1.getResourceId());
        topo.remove(sn1u.getResourceId());
        topo.remove(dc1u.getResourceId());

        assertRemoved(dc1u, sn1u, rg1, rn1u, p1u);

        /* Verify that remove changes can be applied. */
        verifyApplyChangelist(topo, incTopo);
    }

    /**
     * Verify the change list apis
     */
    @Test
    public void testChangeLists() {

        Datacenter dc1 = topo.add(
            Datacenter.newInstance("EC-datacenter", 2,
                                   DatacenterType.PRIMARY, false, false));
        Datacenter dc2 = topo.add(
            Datacenter.newInstance("WC-datacenter", 1,
                                   DatacenterType.SECONDARY, false, false));

        StorageNode sn1 = topo.add(new StorageNode(dc1,
                                                  "sn1-hostname",
                                                  Registry.REGISTRY_PORT));
        StorageNode sn2 = topo.add(new StorageNode(dc2,
                                                   "sn2-hostname",
                                                   Registry.REGISTRY_PORT));
        RepGroup rg1 = topo.add(new RepGroup());

        rg1.add(new RepNode(sn1.getResourceId()));
        rg1.add(new RepNode(sn2.getResourceId()));

        topo.add(new Partition(rg1));
        topo.add(new Partition(rg1));

        List<TopologyChange> allChanges = topo.getChanges(1);
        Topology topo2 = new Topology(topo.getKVStoreName());
        topo2.apply(allChanges);

        assertTopoEquals(topo, topo2);

        /* Discards the first three changes. */
        final int discardSeqNum = 3;
        topo.discardChanges(discardSeqNum);
        for (int i=0; i <= discardSeqNum; i++) {
            List<TopologyChange> changes = topo.getChanges(i);
            assertNull(changes);
        }
        /* No exception. */
        topo.getChanges(discardSeqNum+1);
    }

    /**
     * Verifies that changes can be applied, both to an empty Topology and
     * incrementally to an obsolete Topology that has some, but not all the
     * changes.
     */
    private void verifyApplyChangelist(Topology refTopo,
                               Topology incTopo)
        throws IOException, ClassNotFoundException {
        verifySerialization(refTopo);

        Topology topo2= new Topology(refTopo.getKVStoreName());
        topo2.apply(refTopo.getChanges(1));
        assertTopoEquals(refTopo, topo2);
        verifySerialization(topo2);

        incTopo.apply(refTopo.getChanges(incTopo.getSequenceNumber()+1));
        assertTopoEquals(refTopo, incTopo);
        verifySerialization(incTopo);
    }

    /**
     * Check whether two topologies have the same contents by comparing
     * their serialized byte contents.
     */
    public static void assertTopoEquals(Topology topo1, Topology topo2) {
        assertTopoEquals(topo1, topo2, SerialVersion.CURRENT, false);
    }

    /**
     * Check whether two topologies have the same contents by comparing
     * their serialized byte contents.
     */
    public static void assertTopoEquals(Topology topo1,
                                        Topology topo2,
                                        short serialVersion) {
        assertTopoEquals(topo1, topo2, serialVersion, false);
    }

    /**
     * Check whether two topologies have the same contents by comparing their
     * serialized byte contents.
     */
    public static void assertTopoEquals(Topology topo1,
                                        Topology topo2,
                                        boolean ignoreSequenceNumber) {
        assertTopoEquals(
            topo1, topo2, SerialVersion.CURRENT, ignoreSequenceNumber);
    }

    /**
     * Check whether two topologies have the same contents by comparing their
     * serialized byte contents, using the serial version to decide what to
     * check.
     */
    public static void assertTopoEquals(Topology topo1,
                                        Topology topo2,
                                        short serialVersion,
                                        boolean ignoreSequenceNumber) {

        assertEquals("Version", topo1.getVersion(), topo2.getVersion());
        assertEquals("KVStoreName",
                     topo1.getKVStoreName(), topo2.getKVStoreName());
        if (serialVersion >= SerialVersion.MINIMUM) {
            if (!ignoreSequenceNumber) {
                assertEquals(
                    "SequenceNumber",
                    topo1.getSequenceNumber(), topo2.getSequenceNumber());
            }
            assertArrayEquals("Signature",
                              topo1.getSignature(), topo2.getSignature());
        }

        ComponentMap<?, ?>[] maps1 = topo1.getAllComponentMaps();
        ComponentMap<?, ?>[] maps2 = topo2.getAllComponentMaps();
        assertEquals(maps1.length, maps2.length);
        for (int i=0; i < maps1.length; i++) {
            assertEquals("(1)=" + maps1[i] + "\n(2)=" + maps2[i],
                         maps1[i], maps2[i]);
        }
    }

    /* verifies components that are supposed to be present */
    private void assertPresent(Component<?> ... cs) {
        for (Component<?> c : cs) {
            Component<?> cl = topo.get(c.getResourceId());
            assertTrue (c == cl);
        }
    }

    /* Verifies that components have been replaced as expected. */
    private void assertReplaced(Component<?> ... cs) {
        for (Component<?> c : cs) {
            assertTrue (c.getTopology() == null);
            ResourceId cId = c.getResourceId();
            Component<?> cl = topo.get(cId);
            assertTrue (c != cl);
        }
    }

    /* Verifies that components have been removed as expected */
    private void assertRemoved(Component<?> ... cs) {
        for (Component<?> c : cs) {
            assertTrue (c.getTopology() == null);
            ResourceId cId = c.getResourceId();
            assertNull(topo.get(cId));
        }
    }

    /* verifies that the Topology can be serialized and de-serialized */
    private void verifySerialization(Topology stopo)
        throws IOException, ClassNotFoundException {

        final Topology topo1 = serialize(stopo);
        assertTopoEquals(stopo, topo1);
        assertEquals("Id", stopo.getId(), topo1.getId());

        final Topology topo2 = fastSerialize(stopo);
        assertTopoEquals(stopo, topo2);
        assertEquals("Id", stopo.getId(), topo2.getId());
    }

    @Test
    public void testVerbosePartitionDisplay() {

        List<PartitionId> partIds = new ArrayList<>();
        partIds.add(new PartitionId(100));
        partIds.add(new PartitionId(5));
        partIds.add(new PartitionId(4));
        partIds.add(new PartitionId(20));
        partIds.add(new PartitionId(21));
        partIds.add(new PartitionId(22));
        partIds.add(new PartitionId(9));

        assertEquals("4-5,9,20-22,100", TopologyPrinter.listPartitions(partIds));
    }

    @Test
    public void testFastExternalSerilizedSize() {
        createForSerialization();
        final byte[] bytes =
            SerializationUtil.getBytes(topo, SerialVersion.CURRENT);
        assertTrue(
            String.format(
                "Expected serialization size around %s, got %s. "
                + "Please adjust %s accordingly.",
                28000, bytes.length,
                TopologyHistoryCache.class.getName()),
            (28000 <= bytes.length) && (bytes.length <= 29000));
    }

    /**
     * Constructs a topology with 3 SNs, 16 RGs and rf=3 with 48 RNs in total.
     * Each RG has 125 partitions and thus in total, 125 * 16 = 2000
     * partitions. This is close to the most common shape in the cloud.
     */
    private void createForSerialization() {
        final int numSNs = 3;
        final int numRGs = 16;
        final int rf = 3;
        final int numPartitionsPerRG = 125;
        final Datacenter dc1 = topo.add(
            Datacenter.newInstance(
                "EC-datacenter", 3,
                DatacenterType.PRIMARY, false, false));
        final StorageNode[] sns = new StorageNode[numSNs];
        for (int i = 0; i < numSNs; ++i) {
            sns[i] = topo.add(
                new StorageNode(dc1,
                                String.format("sn%s-hostname", i + 1),
                                TestUtils.DUMMY_REGISTRY_PORT));

        }
        final RepGroup[] rgs = new RepGroup[numRGs];
        for (int i = 0; i < numRGs; ++i) {
            rgs[i] = topo.add(new RepGroup());
        }
        for (int i = 0; i < rf; ++i) {
            for (int j = 0; j < numRGs; ++j) {
                rgs[j].add(new RepNode(sns[i].getResourceId()));
            }
        }
        for (int i = 0; i < numRGs; ++i) {
            for (int j = 0; j < numPartitionsPerRG; ++j) {
                topo.add(new Partition(rgs[i]));
            }
        }
        topo.pruneChanges(Integer.MAX_VALUE, 0);
    }

    @Test
    public void testJavaSerilizedSize() {
        createForSerialization();
        final byte[] bytes =
            SerializationUtil.getBytes(topo);
        assertTrue(
            String.format(
                "Expected serialization size around %s, got %s. "
                + "Please adjust %s accordingly.",
                76000, bytes.length,
                TopologyHistoryCache.class.getName()),
            (76000 <= bytes.length) && (bytes.length <= 77000));
    }

    @Test
    public void testDatacenterNewInstance() {
        /* DatacenterType */
        checkException(
            () -> Datacenter.newInstance("dc1",
                                         1, /* repFactor */
                                         null, /* datacenterType */
                                         true, /* allowArbiters */
                                         true /* masterAffinity */),
            IllegalArgumentException.class,
            "datacenterType must not be null");

        /* RepFactor */
        checkException(
            () -> Datacenter.newInstance("dc1",
                                         -1, /* repFactor */
                                         DatacenterType.PRIMARY,
                                         true, /* allowArbiters */
                                         false /* masterAffinity */),
            IllegalArgumentException.class,
            "Replication factor must be greater than or equal to 0");

        /* AllowArbiters */
        checkException(
            () -> Datacenter.newInstance("dc1",
                                         0, /* repFactor */
                                         DatacenterType.PRIMARY,
                                         false, /* allowArbiters */
                                         false /* masterAffinity */),
            IllegalArgumentException.class,
            "allowArbiters was false but should be true");
        assertTrue(Datacenter.newInstance("dc1",
                                          0, /* repFactor */
                                          DatacenterType.PRIMARY,
                                          true, /* allowArbiters */
                                          false /* masterAffinity */)
                   .getAllowArbiters());
        assertFalse(Datacenter.newInstance("dc1",
                                           1, /* repFactor */
                                           DatacenterType.PRIMARY,
                                           false, /* allowArbiters */
                                           false /* masterAffinity */)
                    .getAllowArbiters());
        assertTrue(Datacenter.newInstance("dc1",
                                          1, /* repFactor */
                                          DatacenterType.PRIMARY,
                                          true, /* allowArbiters */
                                          false /* masterAffinity */)
                   .getAllowArbiters());
        assertFalse(Datacenter.newInstance("dc1",
                                           1, /* repFactor */
                                           DatacenterType.SECONDARY,
                                           false, /* allowArbiters */
                                           false /* masterAffinity */)
                    .getAllowArbiters());
        checkException(
            () -> Datacenter.newInstance("dc1",
                                         1, /* repFactor */
                                         DatacenterType.SECONDARY,
                                         true, /* allowArbiters */
                                         false /* masterAffinity */),
            IllegalArgumentException.class,
            "allowArbiters was true but should be false");

        /* MasterAffinity */
        assertFalse(Datacenter.newInstance("dc1",
                                           0, /* repFactor */
                                           DatacenterType.PRIMARY,
                                           true, /* allowArbiters */
                                           false /* masterAffinity */)
                    .getMasterAffinity());
        checkException(
            () -> Datacenter.newInstance("dc1",
                                         0, /* repFactor */
                                         DatacenterType.PRIMARY,
                                         true, /* allowArbiters */
                                         true /* masterAffinity */),
            IllegalArgumentException.class,
            "masterAffinity was true but should be false");
        assertFalse(Datacenter.newInstance("dc1",
                                           1, /* repFactor */
                                           DatacenterType.PRIMARY,
                                           true, /* allowArbiters */
                                           false /* masterAffinity */)
                   .getMasterAffinity());
        assertTrue(Datacenter.newInstance("dc1",
                                          1, /* repFactor */
                                          DatacenterType.PRIMARY,
                                          true, /* allowArbiters */
                                          true /* masterAffinity */)
                   .getMasterAffinity());
        assertFalse(Datacenter.newInstance("dc1",
                                           1, /* repFactor */
                                           DatacenterType.SECONDARY,
                                           false, /* allowArbiters */
                                           false /* masterAffinity */)
                    .getMasterAffinity());
        checkException(
            () -> Datacenter.newInstance("dc1",
                                         1, /* repFactor */
                                         DatacenterType.SECONDARY,
                                         false, /* allowArbiters */
                                         true /* masterAffinity */),
            IllegalArgumentException.class,
            "masterAffinity was true but should be false");
    }

    @Test
    public void testDatacenterComputeAllowArbiters() {
        assertTrue(
            Datacenter.computeAllowArbiters(
                false, DatacenterType.PRIMARY, 0));
        assertTrue(
            Datacenter.computeAllowArbiters(
                true, DatacenterType.PRIMARY, 0));
        assertFalse(
            Datacenter.computeAllowArbiters(
                false, DatacenterType.PRIMARY, 1));
        assertTrue(
            Datacenter.computeAllowArbiters(
                true, DatacenterType.PRIMARY, 1));
        assertFalse(
            Datacenter.computeAllowArbiters(
                false, DatacenterType.SECONDARY, 0));
        assertFalse(
            Datacenter.computeAllowArbiters(
                true, DatacenterType.SECONDARY, 0));
        assertFalse(
            Datacenter.computeAllowArbiters(
                false, DatacenterType.SECONDARY, 1));
        assertFalse(
            Datacenter.computeAllowArbiters(
                true, DatacenterType.SECONDARY, 1));
    }

    @Test
    public void testDatacenterComputeMasterAffinity() {
        assertFalse(
            Datacenter.computeMasterAffinity(
                false, DatacenterType.PRIMARY, 0));
        assertFalse(
            Datacenter.computeMasterAffinity(
                true, DatacenterType.PRIMARY, 0));
        assertFalse(
            Datacenter.computeMasterAffinity(
                false, DatacenterType.PRIMARY, 1));
        assertTrue(
            Datacenter.computeMasterAffinity(
                true, DatacenterType.PRIMARY, 1));
        assertFalse(
            Datacenter.computeMasterAffinity(
                false, DatacenterType.SECONDARY, 0));
        assertFalse(
            Datacenter.computeMasterAffinity(
                true, DatacenterType.SECONDARY, 0));
        assertFalse(
            Datacenter.computeMasterAffinity(
                false, DatacenterType.SECONDARY, 1));
        assertFalse(
            Datacenter.computeMasterAffinity(
                true, DatacenterType.SECONDARY, 1));
    }

    @Test
    public void testLocalization() {
        final Datacenter dc1 =
            topo.add(Datacenter.newInstance(
                "EC-datacenter", 2, DatacenterType.PRIMARY, false, false));
        final StorageNode sn1 =
            topo.add(new StorageNode(
                dc1, "sn1-hostname", TestUtils.DUMMY_REGISTRY_PORT));
        final RepGroup rg1 = topo.add(new RepGroup());
        final RepGroup rg2 = topo.add(new RepGroup());
        rg1.add(new RepNode(sn1.getResourceId()));
        rg1.add(new RepNode(sn1.getResourceId()));
        final Partition p1 = topo.add(new Partition(rg1));
        assertEquals(7, topo.getSequenceNumber());
        assertEquals(Topology.NULL_LOCALIZATION, topo.getLocalizationNumber());
        topo.updatePartitionLocalized(p1.getResourceId(), rg2.getResourceId());
        assertEquals(7, topo.getSequenceNumber());
        assertEquals(0, topo.getLocalizationNumber());
        topo.updatePartitionLocalized(p1.getResourceId(), rg1.getResourceId());
        assertEquals(7, topo.getSequenceNumber());
        assertEquals(1, topo.getLocalizationNumber());
    }

    @Test
    public void testIsPartitionName() {
        assertTrue(PartitionId.isPartitionName("p000000000001"));
        assertTrue(PartitionId.isPartitionName("p-000000000001"));
        assertTrue(PartitionId.isPartitionName("p12345"));
        assertTrue(PartitionId.isPartitionName("p-12345"));
        assertTrue(PartitionId.isPartitionName("p2147483647"));
        assertTrue(PartitionId.isPartitionName("p+2147483647"));
        assertTrue(PartitionId.isPartitionName("p-2147483648"));
        assertTrue(!PartitionId.isPartitionName("p2147483648"));
        assertTrue(!PartitionId.isPartitionName("p21474836470"));
        assertTrue(!PartitionId.isPartitionName("p-2147483649"));
        assertTrue(!PartitionId.isPartitionName("p-21474836480"));
        assertTrue(!PartitionId.isPartitionName(""));
        assertTrue(!PartitionId.isPartitionName("p"));
        assertTrue(!PartitionId.isPartitionName("p-"));
        assertTrue(!PartitionId.isPartitionName("p+"));
        assertTrue(!PartitionId.isPartitionName("12345"));
        assertTrue(!PartitionId.isPartitionName("-12345"));
        assertTrue(!PartitionId.isPartitionName("p%12345"));
        assertTrue(!PartitionId.isPartitionName("p123!45"));
        assertTrue(!PartitionId.isPartitionName("p123a45"));
        assertTrue(!PartitionId.isPartitionName("P12345"));
    }
}
