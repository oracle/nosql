/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.topo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.topo.Validations.EmptyZone;
import oracle.kv.impl.admin.topo.Validations.ExcessAdmins;
import oracle.kv.impl.admin.topo.Validations.ExcessRNs;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.NoPartition;
import oracle.kv.impl.admin.topo.Validations.NoPrimaryDC;
import oracle.kv.impl.admin.topo.Validations.NonOptimalNumPartitions;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.RNHeapExceedsSNMemory;
import oracle.kv.impl.admin.topo.Validations.RNProximity;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.admin.topo.Validations.WrongAdminType;
import oracle.kv.impl.admin.topo.Validations.WrongNodeType;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

import org.junit.Test;

import com.sleepycat.je.rep.NodeType;

/**
 * Test Validations classes.  Emphasis here is on the equals(), hashCode()
 * and accessors.
 */
public class ValidationsTest extends TestBase {

    interface HelperDef<P extends RulesProblem> {
        P build();
    }

    /*
     * Function to do checking of equality, hashCode and basic toString
     * checking for an array of problem definitions, each of which should
     * be different in some way from all others.
     */
    /*
     * Disable unlikely argument type warnings because we are deliberately
     * testing equals calls with unlikely arguments.
     */
    @SuppressWarnings("unlikely-arg-type")
    <P extends RulesProblem> void checkDefs(HelperDef<P>[] defs) {
        for (HelperDef<P> def : defs) {
            final P prob = def.build();

            for (HelperDef<P> odef : defs) {
                final P oprob = odef.build();
                if (def == odef) {
                    /* Compare against self */
                    assertEquals(prob.hashCode(), prob.hashCode());
                    assertEquals(prob, prob);

                    /* Compare against equal */
                    assertEquals(prob.hashCode(), oprob.hashCode());
                    assertEquals(prob, oprob);

                    /* Compare against null */
                    /* CHECKSTYLE:OFF - I want the literal on the right */
                    assertFalse(prob.equals(null));
                    /* CHECKSTYLE:ON */

                    /* Compare against wrong type */
                    assertFalse(prob.equals(def));
                    /* CHECKSTYLE:OFF - I want the literal on the right */
                    assertFalse(prob.equals("Foo"));
                    /* CHECKSTYLE:ON */

                    assertTrue(prob.toString() != null);
                } else {
                    /*
                     * For tests against others, simply check that the
                     * definitions yield Problems that are non-equal.
                     */
                    assertFalse(prob.equals(oprob));
                }
            }
        }
    }

    /* Helper for testInsufficientRNs */
    private static final class IRNDef implements HelperDef<InsufficientRNs> {
        private final DatacenterId dcId;
        private final int reqRF;
        private final RepGroupId rgId;
        private final int numMiss;

        private IRNDef(
            DatacenterId dcId, int reqRF, RepGroupId rgId, int numMiss) {
            this.dcId = dcId;
            this.reqRF = reqRF;
            this.rgId = rgId;
            this.numMiss = numMiss;
        }

        @Override
        public InsufficientRNs build() {
            return new InsufficientRNs(dcId, reqRF, rgId, numMiss);
        }
    }

    @Test
    public void testInsufficientRNs() {
        final int DC_ID = 5;
        final int RG_ID = 9;
        final int REQ_RF = 1;
        final int NUM_MISS = 0;

        /* Each entry should differ from every other entry */
        final IRNDef[] irnDefs = new IRNDef[] {
            /* The "typical" instance */
            new IRNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_MISS),
            /* null SN ID */
            new IRNDef(null,
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_MISS),
            /* null RG ID */
            new IRNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       null,
                       NUM_MISS),
            /* All null resource IDs */
            new IRNDef(null,
                       REQ_RF,
                       null,
                       NUM_MISS),
            /* Differ in DatacenterId */
            new IRNDef(new DatacenterId(DC_ID + 1),
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_MISS),
            /* Differ in requiredRF */
            new IRNDef(new DatacenterId(DC_ID),
                       REQ_RF + 1,
                       new RepGroupId(RG_ID),
                       NUM_MISS),
            /* Differ in RepGroupId */
            new IRNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       new RepGroupId(RG_ID + 1),
                       NUM_MISS),
            /* Differ in numMissing */
            new IRNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_MISS + 1)
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(irnDefs);

        /* Check methods of the typical instance */
        final InsufficientRNs typicalIRN = irnDefs[0].build();
        assertEquals(typicalIRN.getResourceId(), new RepGroupId(RG_ID));
        assertTrue(typicalIRN.isViolation());
        assertEquals(typicalIRN.getDCId(), new DatacenterId(DC_ID));
        assertEquals(typicalIRN.getNumNeeded(), NUM_MISS);
        assertEquals(typicalIRN.getRGId(), new RepGroupId(RG_ID));
    }

    /* Helper for testExcessRNs */
    private static final class ERNDef implements HelperDef<ExcessRNs> {
        private final DatacenterId dcId;
        private final int reqRF;
        private final RepGroupId rgId;
        private final int numExcess;

        private ERNDef(
            DatacenterId dcId, int reqRF, RepGroupId rgId, int numExcess) {
            this.dcId = dcId;
            this.reqRF = reqRF;
            this.rgId = rgId;
            this.numExcess = numExcess;
        }

        @Override
        public ExcessRNs build() {
            return new ExcessRNs(dcId, reqRF, rgId, numExcess);
        }
    }

    @Test
    public void testExcessRNs() {
        final int DC_ID = 5;
        final int RG_ID = 9;
        final int REQ_RF = 1;
        final int NUM_EXCESS = 0;

        /* Each entry should differ from every other entry */
        final ERNDef[] ernDefs = new ERNDef[] {
            /* The "typical" instance */
            new ERNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_EXCESS),
            /* null dc id */
            new ERNDef(null,
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_EXCESS),
            /* null rg id */
            new ERNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       null,
                       NUM_EXCESS),
            /* all ids null */
            new ERNDef(null,
                       REQ_RF,
                       null,
                       NUM_EXCESS),
            /* Differ in DatacenterId */
            new ERNDef(new DatacenterId(DC_ID + 1),
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_EXCESS),
            /* Differ in requiredRF */
            new ERNDef(new DatacenterId(DC_ID),
                       REQ_RF + 1,
                       new RepGroupId(RG_ID),
                       NUM_EXCESS),
            /* Differ in RepGroupId */
            new ERNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       new RepGroupId(RG_ID + 1),
                       NUM_EXCESS),
            /* Differ in numExcess */
            new ERNDef(new DatacenterId(DC_ID),
                       REQ_RF,
                       new RepGroupId(RG_ID),
                       NUM_EXCESS + 1)
            };

        /* equals(), hashCode(), toString() checks */
        checkDefs(ernDefs);

        final ExcessRNs typicalERN = ernDefs[0].build();
        /* CHECKSTYLE:OFF - I want the literal on the right */
        assertFalse(typicalERN == null);

        /* CHECKSTYLE:ON */

        assertEquals(typicalERN.getResourceId(), new RepGroupId(RG_ID));
        assertFalse(typicalERN.isViolation());
    }

    /* Helper for testRNProximity */
    private static final class RNPDef implements HelperDef<RNProximity> {
        private final StorageNodeId snId;
        private final RepGroupId rgId;
        private final List<RepNodeId> rnList;

        private RNPDef(
            StorageNodeId snId, RepGroupId rgId, List<RepNodeId> rnList) {
            this.snId = snId;
            this.rgId = rgId;
            this.rnList = rnList;
        }

        @Override
        public RNProximity build() {
            return new RNProximity(snId, rgId, rnList);
        }
    }

    @Test
    public void testRNProximity() {
        final int SN_ID = 5;
        final int RG_ID = 9;
        final int RN_ID_1 = 1;
        final int RN_ID_2 = 2;

        /* Each entry should differ from every other entry */
        final RNPDef[] rnpDefs = new RNPDef[] {
            /* The "typical" instance */
            new RNPDef(new StorageNodeId(SN_ID),
                       new RepGroupId(RG_ID),
                       Arrays.asList(new RepNodeId(RG_ID, RN_ID_1),
                                     new RepNodeId(RG_ID, RN_ID_2))),
            /* null sn id */
            new RNPDef(null,
                       new RepGroupId(RG_ID),
                       Arrays.asList(new RepNodeId(RG_ID, RN_ID_1),
                                     new RepNodeId(RG_ID, RN_ID_2))),
            /* null rg id */
            new RNPDef(new StorageNodeId(SN_ID),
                       null,
                       Arrays.asList(new RepNodeId(RG_ID, RN_ID_1),
                                     new RepNodeId(RG_ID, RN_ID_2))),
            /* null list */
            new RNPDef(new StorageNodeId(SN_ID),
                       new RepGroupId(RG_ID),
                       null),
            /* all null ids */
            new RNPDef(null,
                       null,
                       null),
            /* Differ in StorageNodeId */
            new RNPDef(new StorageNodeId(SN_ID + 1),
                       new RepGroupId(RG_ID),
                       Arrays.asList(new RepNodeId(RG_ID, RN_ID_1),
                                     new RepNodeId(RG_ID, RN_ID_2))),
            /* Differ in RepGroupId */
            new RNPDef(new StorageNodeId(SN_ID),
                       new RepGroupId(RG_ID + 1),
                       Arrays.asList(new RepNodeId(RG_ID, RN_ID_1),
                                     new RepNodeId(RG_ID, RN_ID_2))),
            /* Too few RepNodeIDs */
            new RNPDef(new StorageNodeId(SN_ID),
                       new RepGroupId(RG_ID),
                       Arrays.asList(new RepNodeId(RG_ID, RN_ID_1)))
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(rnpDefs);

        final RNProximity typicalRNP = rnpDefs[0].build();
        assertEquals(typicalRNP.getResourceId(), new StorageNodeId(SN_ID));
        assertTrue(typicalRNP.isViolation());
        assertEquals(typicalRNP.getSNId(), new StorageNodeId(SN_ID));
        final List<RepNodeId> rnList = typicalRNP.getRNList();
        assertEquals(rnList.size(), 2);
        assertEquals(rnList.get(0), new RepNodeId(RG_ID, RN_ID_1));
        assertEquals(rnList.get(1), new RepNodeId(RG_ID, RN_ID_2));
    }

    /* Helper for testOverCapacity */
    private static final class OCDef implements HelperDef<OverCapacity> {
        private final StorageNodeId snId;
        private final int rnCount;
        private final int capacity;

        private OCDef(StorageNodeId snId, int rnCount, int capacity) {
            this.snId = snId;
            this.rnCount = rnCount;
            this.capacity = capacity;
        }

        @Override
        public OverCapacity build() {
            return new OverCapacity(snId, rnCount, capacity);
        }
    }

    @Test
    public void testOverCapacity() {
        final int SN_ID = 5;
        final int RN_COUNT = 3;
        final int CAP = 2;

        final OCDef[] ocDefs = new OCDef[] {
            /* The "typical" instance */
            new OCDef(new StorageNodeId(SN_ID), RN_COUNT, CAP),
            /* null  StorageNodeId */
            new OCDef(null, RN_COUNT, CAP),
            /* Differ in StorageNodeId */
            new OCDef(new StorageNodeId(SN_ID + 1), RN_COUNT, CAP),
            /* Differ in RN count */
            new OCDef(new StorageNodeId(SN_ID), RN_COUNT + 1, CAP),
            /* Differ in Capacity */
            new OCDef(new StorageNodeId(SN_ID), RN_COUNT, CAP + 1),
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(ocDefs);

        final OverCapacity typicalOC = ocDefs[0].build();
        assertEquals(typicalOC.getResourceId(), new StorageNodeId(SN_ID));
        assertTrue(typicalOC.isViolation());
        assertEquals(typicalOC.getExcess(), (RN_COUNT - CAP));
        assertEquals(typicalOC.getSNId(), new StorageNodeId(SN_ID));
    }

    /* Helper for testRNHeapExceesSNMemory */
    private static final class RNHESNMDef
        implements HelperDef<RNHeapExceedsSNMemory> {
        private final StorageNodeId snId;
        private final int memMB;
        private final Set<RepNodeId> rnIds;
        private final int rnMemMB;
        private final String rnMemList;

        private RNHESNMDef(StorageNodeId snId,
                           int memMB,
                           Set<RepNodeId> rnIds,
                           int rnMemMB,
                           String rnMemList) {
            this.snId = snId;
            this.memMB = memMB;
            this.rnIds = rnIds;
            this.rnMemMB = rnMemMB;
            this.rnMemList = rnMemList;
        }

        @Override
        public RNHeapExceedsSNMemory build() {
            return new RNHeapExceedsSNMemory(
                snId, memMB, rnIds, rnMemMB, rnMemList);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRNHeapExceedsSNMemory() {
        final int SN_ID = 2;
        final int MEM_MB = 3000;
        final int RN_MEM_MB = 4000;
        final int RG_ID = 1;
        final int RN_ID_1 = 3;
        final int RN_ID_2 = 4;
        final int RN_ID_3 = 5;
        final String RN_MEM_LIST = "3 1000MB, 4 1000MB,";

        final HashSet<RepNodeId> minusRNSet = new HashSet<RepNodeId>();
        minusRNSet.add(new RepNodeId(RG_ID, RN_ID_1));
        final HashSet<RepNodeId> typicalRNSet =
            (HashSet<RepNodeId>) minusRNSet.clone();
        typicalRNSet.add(new RepNodeId(RG_ID, RN_ID_2));
        final HashSet<RepNodeId> plusRNSet =
            (HashSet<RepNodeId>) typicalRNSet.clone();
        plusRNSet.add(new RepNodeId(RG_ID, RN_ID_3));


        final RNHESNMDef[] rnhesnmDefs = new RNHESNMDef[] {
            /* The "typical" instance */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           typicalRNSet,
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* null StorageNodeId */
            new RNHESNMDef(null,
                           MEM_MB,
                           (Set<RepNodeId>) typicalRNSet.clone(),
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* null rn set */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           null,
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* null mem list */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           typicalRNSet,
                           RN_MEM_MB,
                           null),
            /* all null ids */
            new RNHESNMDef(null,
                           MEM_MB,
                           null,
                           RN_MEM_MB,
                           null),
            /* Differ in StorageNodeId */
            new RNHESNMDef(new StorageNodeId(SN_ID + 1),
                           MEM_MB,
                           (Set<RepNodeId>) typicalRNSet.clone(),
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* Differ in mem */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB + 100,
                           (Set<RepNodeId>) typicalRNSet.clone(),
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* Differ in RN set (subset) */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           (Set<RepNodeId>) minusRNSet.clone(),
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* Differ in RN set (superset) */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           (Set<RepNodeId>) plusRNSet.clone(),
                           RN_MEM_MB,
                           RN_MEM_LIST),
            /* Differ in RN mem total */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           (Set<RepNodeId>) typicalRNSet.clone(),
                           RN_MEM_MB + 100,
                           RN_MEM_LIST),
            /* Differ in mem list */
            new RNHESNMDef(new StorageNodeId(SN_ID),
                           MEM_MB,
                           (Set<RepNodeId>) typicalRNSet.clone(),
                           RN_MEM_MB,
                           RN_MEM_LIST + "extra")
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(rnhesnmDefs);

        final RNHeapExceedsSNMemory typicalRNHESNM =
            rnhesnmDefs[0].build();
        assertEquals(typicalRNHESNM.getResourceId(), new StorageNodeId(SN_ID));
        assertTrue(typicalRNHESNM.isViolation());
    }

    /* Helper for testUnderCapacity */
    private static final class UCDef implements HelperDef<UnderCapacity> {
        private final StorageNodeId snId;
        int rnCount;
        int capacity;

        private UCDef(StorageNodeId snId, int rnCount, int capacity) {
            this.snId = snId;
            this.rnCount = rnCount;
            this.capacity = capacity;
        }

        @Override
        public UnderCapacity build() {
            return new UnderCapacity(snId, rnCount, capacity);
        }
    }

    @Test
    public void testUnderCapacity() {
        final int SN_ID = 5;
        final int RN_COUNT = 2;
        final int CAP = 3;

        final UCDef[] ucDefs = new UCDef[] {
            /* The "typical" instance */
            new UCDef(new StorageNodeId(SN_ID), RN_COUNT, CAP),
            /* null StorageNodeId */
            new UCDef(null, RN_COUNT, CAP),
            /* Differ in StorageNodeId */
            new UCDef(new StorageNodeId(SN_ID + 1), RN_COUNT, CAP),
            /* Differ in RN count */
            new UCDef(new StorageNodeId(SN_ID), RN_COUNT + 1, CAP),
            /* Differ in Capacity */
            new UCDef(new StorageNodeId(SN_ID), RN_COUNT, CAP + 1),
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(ucDefs);

        final UnderCapacity typicalUC = ucDefs[0].build();
        assertEquals(typicalUC.getResourceId(), new StorageNodeId(SN_ID));
        assertFalse(typicalUC.isViolation());
    }

    /* Helper for testNonOptimalNumPartitions */
    private static final class NONPDef
        implements HelperDef<NonOptimalNumPartitions> {

        private final RepGroupId rgId;
        private final int count;
        private final int minCount;
        private final int maxCount;

        private NONPDef(
            RepGroupId rgId, int count, int minCount, int maxCount) {
            this.rgId = rgId;
            this.count = count;
            this.minCount = minCount;
            this.maxCount = maxCount;
        }

        @Override
        public NonOptimalNumPartitions build() {
            return new NonOptimalNumPartitions(rgId, count, minCount, maxCount);
        }
    }

    @Test
    public void testNonOptimalNumPartitions() {
        final int RG_ID = 5;
        final int COUNT = 50;
        final int MIN_COUNT = 20;
        final int MAX_COUNT = 100;

        final NONPDef[] nonpDefs = new NONPDef[] {
            /* The "typical" instance */
            new NONPDef(new RepGroupId(RG_ID), COUNT, MIN_COUNT, MAX_COUNT),
            /* null RepGroupId */
            new NONPDef(null, COUNT, MIN_COUNT, MAX_COUNT),
            /* Differ in RepGroupId */
            new NONPDef(new RepGroupId(RG_ID + 1), COUNT, MIN_COUNT, MAX_COUNT),
            /* Differ in count */
            new NONPDef(new RepGroupId(RG_ID), COUNT + 1, MIN_COUNT, MAX_COUNT),
            /* Differ in min count */
            new NONPDef(new RepGroupId(RG_ID), COUNT, MIN_COUNT + 1, MAX_COUNT),
            /* Differ in max count */
            new NONPDef(new RepGroupId(RG_ID), COUNT + 1, MIN_COUNT,
                        MAX_COUNT + 1),
            /* All counts equal */
            new NONPDef(new RepGroupId(RG_ID), COUNT, COUNT, COUNT)
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(nonpDefs);

        final NonOptimalNumPartitions typicalNONP = nonpDefs[0].build();
        assertEquals(typicalNONP.getResourceId(), new RepGroupId(RG_ID));
        assertFalse(typicalNONP.isViolation());
    }

    /* Helper for testMultipleRNsInRoot */
    private final class MRNIRDef implements HelperDef<MultipleRNsInRoot> {
        private final StorageNodeId snId;
        private final List<RepNodeId> rnIds;
        private final String rootDir;

        private MRNIRDef(
            StorageNodeId snId, List<RepNodeId> rnIds, String rootDir) {
            this.snId = snId;
            this.rnIds = rnIds;
            this.rootDir = rootDir;
        }

        @Override
        public MultipleRNsInRoot build() {
            return new MultipleRNsInRoot(snId, rnIds, rootDir);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultipleRNsInRoot() {
        final int SN_ID = 2;
        final int RG_ID = 1;
        final int RN_ID_1 = 3;
        final int RN_ID_2 = 4;
        final int RN_ID_3 = 5;
        final String ROOT_DIR = "/tmp/kvroot";

        final ArrayList<RepNodeId> minusRNList = new ArrayList<RepNodeId>();
        minusRNList.add(new RepNodeId(RG_ID, RN_ID_1));
        final ArrayList<RepNodeId> typicalRNList =
            (ArrayList<RepNodeId>) minusRNList.clone();
        typicalRNList.add(new RepNodeId(RG_ID, RN_ID_2));
        final ArrayList<RepNodeId> plusRNList =
            (ArrayList<RepNodeId>) typicalRNList.clone();
        plusRNList.add(new RepNodeId(RG_ID, RN_ID_3));

        final MRNIRDef[] mrnirDefs = new MRNIRDef[] {
            /* The "typical" instance */
            new MRNIRDef(new StorageNodeId(SN_ID), typicalRNList, ROOT_DIR),
            /* null StorageNodeId */
            new MRNIRDef(null,
                         (List<RepNodeId>) typicalRNList.clone(),
                         ROOT_DIR),
            /* null rnList */
            new MRNIRDef(new StorageNodeId(SN_ID), null, ROOT_DIR),
            /* null rootDir */
            new MRNIRDef(new StorageNodeId(SN_ID),
                         (List<RepNodeId>) typicalRNList.clone(),
                         null),
            /* null rnList and rootDir*/
            new MRNIRDef(new StorageNodeId(SN_ID), null, null),
            /* Differ in StorageNodeId */
            new MRNIRDef(new StorageNodeId(SN_ID + 1),
                         (List<RepNodeId>) typicalRNList.clone(),
                         ROOT_DIR),
            /* Differ in RN set (subset) */
            new MRNIRDef(new StorageNodeId(SN_ID),
                         minusRNList,
                         ROOT_DIR),
            /* Differ in RN set (superset) */
            new MRNIRDef(new StorageNodeId(SN_ID),
                         plusRNList,
                         ROOT_DIR),
            /* Differ in root dir */
            new MRNIRDef(new StorageNodeId(SN_ID),
                         (List<RepNodeId>) typicalRNList.clone(),
                         ROOT_DIR + "/subdir")
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(mrnirDefs);

        final MultipleRNsInRoot typicalMRNIR = mrnirDefs[0].build();
        assertEquals(typicalMRNIR.getResourceId(), new StorageNodeId(SN_ID));
        assertFalse(typicalMRNIR.isViolation());
    }

    /* Test NoPrimaryDC */

    @Test
    public void testNoPrimaryDC() {
        final int RG_ID = 7;

        final NPDDef[] defs = new NPDDef[] {
            /* The "typical" instance */
            new NPDDef(new RepGroupId(RG_ID)),
            /* Different rgId */
            new NPDDef(new RepGroupId(RG_ID + 1))
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(defs);

        final NoPrimaryDC typical = defs[0].build();
        assertEquals(typical.getResourceId(), new RepGroupId(RG_ID));
        assertTrue(typical.isViolation());
        try {
            @SuppressWarnings("unused")
            final NoPrimaryDC inst = new NoPrimaryDC(null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }

    /* Test NoPartition */

    @Test
    public void testNoPartition() {
        final int RG_ID = 7;

        final NPDef[] defs = new NPDef[] {
            /* The "typical" instance */
            new NPDef(new RepGroupId(RG_ID)),
            /* Different rgId */
            new NPDef(new RepGroupId(RG_ID + 1))
        };

        /* equals(), hashCode(), toString() checks */
        checkDefs(defs);

        final NoPartition typical = defs[0].build();
        assertEquals(typical.getResourceId(), new RepGroupId(RG_ID));
        assertFalse(defs[1].build().getResourceId().
                equals(new RepGroupId(RG_ID)));
        assertFalse(typical.isViolation());
    }

    private final class NPDDef implements HelperDef<NoPrimaryDC> {
        private final RepGroupId rgId;

        private NPDDef(final RepGroupId rgId) {
            this.rgId = rgId;
        }

        @Override
        public NoPrimaryDC build() {
            return new NoPrimaryDC(rgId);
        }
    }

    /* Test WrongNodeType */

    @Test
    public void testWrongNodeType() {
        final int RG_ID = 7;
        final int RN_ID = 42;
        final int DC_ID = 99;

        final WNTDef[] defs = new WNTDef[] {
            /* The "typical" instance */
            new WNTDef(new RepNodeId(RG_ID, RN_ID), NodeType.MONITOR,
                       new DatacenterId(DC_ID), DatacenterType.SECONDARY),
            /* Different RN ID */
            new WNTDef(new RepNodeId(RG_ID, RN_ID + 1), NodeType.MONITOR,
                       new DatacenterId(DC_ID), DatacenterType.SECONDARY),
            /* Different node type */
            new WNTDef(new RepNodeId(RG_ID, RN_ID), NodeType.ELECTABLE,
                       new DatacenterId(DC_ID), DatacenterType.SECONDARY),
            /* Different datacenter ID */
            new WNTDef(new RepNodeId(RG_ID, RN_ID), NodeType.MONITOR,
                       new DatacenterId(DC_ID + 1), DatacenterType.SECONDARY),
            /* Different datacenter type */
            new WNTDef(new RepNodeId(RG_ID, RN_ID), NodeType.MONITOR,
                       new DatacenterId(DC_ID + 1), DatacenterType.PRIMARY),
        };

        /* equals, hashCode, toString checks */
        checkDefs(defs);

        final WrongNodeType typical = defs[0].build();
        assertEquals(typical.getResourceId(), new RepNodeId(RG_ID, RN_ID));
        assertTrue(typical.isViolation());

        /* Null tests */
        try {
            @SuppressWarnings("unused")
            final WrongNodeType wnt = new WrongNodeType(
                null, NodeType.MONITOR, new DatacenterId(DC_ID),
                DatacenterType.SECONDARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            @SuppressWarnings("unused")
            final WrongNodeType wnt = new WrongNodeType(
                new RepNodeId(RG_ID, RN_ID), null, new DatacenterId(DC_ID),
                DatacenterType.SECONDARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            @SuppressWarnings("unused")
            final WrongNodeType wnt = new WrongNodeType(
                new RepNodeId(RG_ID, RN_ID), NodeType.MONITOR, null,
                DatacenterType.SECONDARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            @SuppressWarnings("unused")
            final WrongNodeType wnt = new WrongNodeType(
                new RepNodeId(RG_ID, RN_ID), NodeType.MONITOR,
                new DatacenterId(DC_ID), null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        /* Node type isn't wrong */
        try {
            @SuppressWarnings("unused")
            final WrongNodeType wnt = new WrongNodeType(
                new RepNodeId(RG_ID, RN_ID), NodeType.ELECTABLE,
                new DatacenterId(DC_ID), DatacenterType.PRIMARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }

    private final class WNTDef implements HelperDef<WrongNodeType> {
        private final RepNodeId rnId;
        private final NodeType nodeType;
        private final DatacenterId dcId;
        private final DatacenterType dcType;

        WNTDef(final RepNodeId rnId,
               final NodeType nodeType,
               final DatacenterId dcId,
               final DatacenterType dcType) {
            this.rnId = rnId;
            this.nodeType = nodeType;
            this.dcId = dcId;
            this.dcType = dcType;
        }

        @Override
        public WrongNodeType build() {
            return new WrongNodeType(rnId, nodeType, dcId, dcType);
        }
    }

    /* Test EmptyZone */

    @Test
    public void testEmptyZone() {
        final int DC_ID = 99;

        final EZDef[] defs = new EZDef[] {
            /* The "typical" instance */
            new EZDef(new DatacenterId(DC_ID)),
            /* Different datacenter ID */
            new EZDef(new DatacenterId(DC_ID + 1))
        };

        /* equals, hashCode, toString checks */
        checkDefs(defs);

        final EmptyZone typical = defs[0].build();
        assertEquals(typical.getResourceId(), new DatacenterId(DC_ID));
        assertTrue(typical.isViolation());

        /* Null tests */
        try {
            @SuppressWarnings("unused")
            final EmptyZone ezt = new EmptyZone(null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
    }

    private final class EZDef implements HelperDef<EmptyZone> {
        private final DatacenterId dcId;

        EZDef(final DatacenterId dcId) {
            this.dcId = dcId;
        }

        @Override
        public EmptyZone build() {
            return new EmptyZone(dcId);
        }
    }

    /* Test WrongAdminType */

    @Test
    public void testWrongAdminType() {
        final int ADMIN_ID = 7;
        final int DC_ID = 99;

        final WATDef[] defs = new WATDef[] {
            /* The "typical" instance */
            new WATDef(new AdminId(ADMIN_ID), AdminType.PRIMARY,
                       new DatacenterId(DC_ID), DatacenterType.PRIMARY),
            /* Different Admin ID */
            new WATDef(new AdminId(ADMIN_ID + 1), AdminType.PRIMARY,
                       new DatacenterId(DC_ID), DatacenterType.PRIMARY),
            /* Different admin type */
            new WATDef(new AdminId(ADMIN_ID), AdminType.SECONDARY,
                       new DatacenterId(DC_ID), DatacenterType.PRIMARY),
            /* Different datacenter ID */
            new WATDef(new AdminId(ADMIN_ID), AdminType.PRIMARY,
                       new DatacenterId(DC_ID + 1), DatacenterType.PRIMARY),
            /* Different datacenter type */
            new WATDef(new AdminId(ADMIN_ID), AdminType.PRIMARY,
                       new DatacenterId(DC_ID), DatacenterType.SECONDARY)
        };

        /* equals, hashCode, toString checks */
        checkDefs(defs);

        final WrongAdminType typical = defs[0].build();
        assertEquals(typical.getResourceId(), new AdminId(ADMIN_ID));
        assertTrue(typical.isViolation());

        /* Null tests */
        try {
            @SuppressWarnings("unused")
            final WrongAdminType wat = new WrongAdminType(
                       null, AdminType.PRIMARY,
                       new DatacenterId(DC_ID), DatacenterType.PRIMARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        /* Bad values */
        try {
            @SuppressWarnings("unused")
            final WrongAdminType wat = new WrongAdminType(
                       new AdminId(ADMIN_ID), null,
                       new DatacenterId(DC_ID), DatacenterType.PRIMARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        try {
            @SuppressWarnings("unused")
            final WrongAdminType wat = new WrongAdminType(
                       new AdminId(ADMIN_ID), AdminType.PRIMARY,
                       null, DatacenterType.PRIMARY);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        try {
            @SuppressWarnings("unused")
            final WrongAdminType wat = new WrongAdminType(
                       new AdminId(ADMIN_ID), AdminType.PRIMARY,
                       new DatacenterId(DC_ID), null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
    }

    private final class WATDef implements HelperDef<WrongAdminType> {
        private final AdminId adminId;
        private final AdminType adminType;
        private final DatacenterId dcId;
        private final DatacenterType dcType;

        WATDef(final AdminId adminId,
               final AdminType adminType,
               final DatacenterId dcId,
               final DatacenterType dcType) {
            this.adminId = adminId;
            this.adminType = adminType;
            this.dcId = dcId;
            this.dcType = dcType;
        }

        @Override
        public WrongAdminType build() {
            return new WrongAdminType(adminId, adminType, dcId, dcType);
        }
    }

    /* Test InsufficientAdmins */

    @Test
    public void testInsufficientAdmins() {
        final int DC_ID = 99;
        final int REQ_RF = 3;
        final int NUM_MISSING = 1;

        final IADef[] defs = new IADef[] {
            /* The "typical" instance */
            new IADef(new DatacenterId(DC_ID), REQ_RF, NUM_MISSING),
            /* Different RF */
            new IADef(new DatacenterId(DC_ID), REQ_RF + 1, NUM_MISSING),
            /* Different missing */
            new IADef(new DatacenterId(DC_ID), REQ_RF, NUM_MISSING + 1),
            /* Different datacenter ID */
            new IADef(new DatacenterId(DC_ID + 1), REQ_RF, NUM_MISSING)
        };

        /* equals, hashCode, toString checks */
        checkDefs(defs);

        final InsufficientAdmins typical = defs[0].build();
        assertEquals(typical.getResourceId(), new DatacenterId(DC_ID));
        assertTrue(typical.isViolation());

        /* Null tests */
        try {
            @SuppressWarnings("unused")
            final InsufficientAdmins iat = new InsufficientAdmins(
                                                null, REQ_RF, NUM_MISSING);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        /* Bad values */
        try {
            @SuppressWarnings("unused")
            final InsufficientAdmins iat = new InsufficientAdmins(
                                  new DatacenterId(DC_ID), 0, NUM_MISSING);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        try {
            @SuppressWarnings("unused")
            final InsufficientAdmins iat = new InsufficientAdmins(
                                  new DatacenterId(DC_ID), REQ_RF, 0);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
    }

    private final class IADef implements HelperDef<InsufficientAdmins> {
        private final DatacenterId dcId;
        private final int requiredRF;
        private final int numMissing;

        IADef(final DatacenterId dcId,
              int requiredRF,
              int numMissing) {
            this.dcId = dcId;
            this.requiredRF = requiredRF;
            this.numMissing = numMissing;
        }

        @Override
        public InsufficientAdmins build() {
            return new InsufficientAdmins(dcId, requiredRF, numMissing);
        }
    }

    /* Test ExcessAdmins */

    @Test
    public void testExcessAdmins() {
        final int DC_ID = 99;
        final int REQ_RF = 3;
        final int NUM_EXCESS = 1;

        final EADef[] defs = new EADef[] {
            /* The "typical" instance */
            new EADef(new DatacenterId(DC_ID), REQ_RF, NUM_EXCESS),
            /* Different RF */
            new EADef(new DatacenterId(DC_ID), REQ_RF + 1, NUM_EXCESS),
            /* Different excess */
            new EADef(new DatacenterId(DC_ID), REQ_RF, NUM_EXCESS + 1),
            /* Different datacenter ID */
            new EADef(new DatacenterId(DC_ID + 1), REQ_RF, NUM_EXCESS)
        };

        /* equals, hashCode, toString checks */
        checkDefs(defs);

        final ExcessAdmins typical = defs[0].build();
        assertEquals(typical.getResourceId(), new DatacenterId(DC_ID));
        assertFalse(typical.isViolation());

        /* Null tests */
        try {
            @SuppressWarnings("unused")
            final ExcessAdmins iat = new ExcessAdmins(
                                                null, REQ_RF, NUM_EXCESS);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        try {
            @SuppressWarnings("unused")
            final ExcessAdmins iat = new ExcessAdmins(
                                  new DatacenterId(DC_ID), -1, NUM_EXCESS);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
        try {
            @SuppressWarnings("unused")
            final ExcessAdmins iat = new ExcessAdmins(
                                  new DatacenterId(DC_ID), REQ_RF, 0);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
    }

    private final class EADef implements HelperDef<ExcessAdmins> {
        private final DatacenterId dcId;
        private final int requiredRF;
        private final int numExcess;

        EADef(final DatacenterId dcId,
              int requiredRF,
              int numExcess) {
            this.dcId = dcId;
            this.requiredRF = requiredRF;
            this.numExcess = numExcess;
        }

        @Override
        public ExcessAdmins build() {
            return new ExcessAdmins(dcId, requiredRF, numExcess);
        }
    }

    /* Helper for testNoPartition */
    private static final class NPDef
        implements HelperDef<NoPartition> {

        private final RepGroupId rgId;

        private NPDef(RepGroupId rgId) {
            this.rgId = rgId;
        }

        @Override
        public NoPartition build() {
            return new NoPartition(rgId);
        }
    }
}
