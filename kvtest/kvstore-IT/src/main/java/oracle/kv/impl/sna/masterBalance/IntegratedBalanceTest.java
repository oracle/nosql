/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna.masterBalance;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.param.ParameterState.Type;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MDInfo;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.KVSTestBase;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.util.Ping;

import org.junit.Test;

/**
 * Tests to verify balancing under realistic deployment conditions, with
 * topologies being deployed by the admin.
 */

public class IntegratedBalanceTest extends KVSTestBase {

    public IntegratedBalanceTest() {
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /**
     * Test to check the disabling of master balancing.
     */
    @Test
    public void testDisableMB() throws Exception {

        /*
         * Change the default for master balancing to false and restore it
         * later so as not to perturb subsequent tests.
         */
        Map<String, ParameterState> pmap = ParameterState.getMap();
        ParameterState oldps = pmap.get(ParameterState.COMMON_MASTER_BALANCE);

        Object p = new DefaultParameter().
            create(ParameterState.COMMON_MASTER_BALANCE,
                   "false", Type.BOOLEAN);
        ParameterState newps =
            new ParameterState(oldps.getType(),
                               p,
                               EnumSet.of(Info.SNA, Info.REPNODE,
                                          Info.POLICY, Info.HIDDEN),
                               oldps.getScope(), 0, 0, null);
        pmap.put(ParameterState.COMMON_MASTER_BALANCE, newps);

        /* Create a KVS with master balancing disabled. */
        initTopology(3 /* RF*/,
                     MB_PER_SN, /* MB per SN */
                     3, /* numSNs */
                     3, 3, 3 /* SN capacities */ );

        /* Check that all 3 masters stay on SN1 */
        awaitUnBalancedKVS(new StorageNodeId(1), 3);

        /* Restore the default */
        pmap.put(ParameterState.COMMON_MASTER_BALANCE, oldps);
    }

    /**
     * Check that the KVS stays unbalanced for a period of 5 seconds, with
     * all masters concentrated on SN snId.
     */
    private void awaitUnBalancedKVS(final StorageNodeId snId,
                                    final int targetMasterCount) {

        final boolean ok = new PollCondition(1000, 60000) {
            int stableMasterCount = 0;

            @Override
            protected boolean condition() {
                int masterCount = 0;
                try {
                    for (RepNodeId rnId : topo.getRepNodeIds()) {
                        RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(rnId);
                        RepNodeStatus rns = rna.ping();
                        if (rns.getReplicationState().isMaster()) {
                            if (snId.equals(topo.get(rnId).getStorageNodeId())) {
                              masterCount++;
                            } else {
                                stableMasterCount = 0;
                                return false;
                            }
                        }
                    }

                    if (masterCount == targetMasterCount) {
                        stableMasterCount++;
                    }

                    /*
                     * True if the masters have not changed in the last 5
                     * iterations.
                     */
                    return (stableMasterCount > 10);

                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return false;
            }
        }.await();

        assertTrue(ok);
    }

    /**
     * Check that the KVS stays affinity balance, with specified masters counts
     * on SN snId.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private void awaitAffinityBalancedKVS(final StorageNodeId snId,
                                          final int targetMasterCount) {

        final boolean ok = new PollCondition(1000, 120000) {

            @Override
            protected boolean condition() {
                int masterCount = 0;
                try {
                    for (RepNodeId rnId : topo.getRepNodeIds()) {
                        try {
                            if (snId.equals(topo.get(rnId).
                                                        getStorageNodeId())) {
                                RepNodeAdminAPI rna =
                                                regUtils.getRepNodeAdmin(rnId);
                                RepNodeStatus rns = rna.ping();
                                if (rns.getReplicationState().isMaster()) {
                                    masterCount++;
                                }
                            }
                        } catch (Exception ex) {
                        }
                    }

                    /*
                     * True if the masters counts are as the target value
                     */
                    return (masterCount == targetMasterCount);

                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return false;
            }
        }.await();

        if (!ok) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final PrintStream ps = new PrintStream(baos);
            for (StorageNodeId sni : topo.getStorageNodeIds()) {
                try {
                    StorageNodeAgentAPI sna =
                        regUtils.getStorageNodeAgent(sni);
                    MDInfo mdInfo = sna.getMDInfo();
                    ps.println(topo.get(snId) + " MD:" +
                          ((mdInfo == null) ? Integer.MIN_VALUE : mdInfo.getMD()));
                } catch (Exception ex) {
                }
            }

            Ping.pingTopology(topo, null, false, CommandParser.JSON_V1,
                              ps, null, null, logger);
            String msg = baos.toString();

            fail(msg);
        }
    }

    /**
     * Test to verify that masters are moved from sns with large capacities to
     * sns of capacity 1. This test was motivated by sr22888.
     */
    @Test
    public void test411111111()
        throws Exception {

        initTopology(3    /* RF*/,
                     MB_PER_SN, /* MB per SN */
                     9,   /* numSNs */
                     4, 1, 1, 1, 1, 1, 1, 1, 1 /* SN capacities */ );

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        /* Expect a balanced KVS at the end with all RNs up */
        awaitBalancedKVS();
    }

    /**
     * Master balance across SNs with hetero capacity. In this example,
     * 4 SNs with capacities 1,2,3 and 4 are used. The r2 topology end up
     * looking like the one below:
     *
     * sn1[rg3-rn3]
     * sn2 [rg1-rn2, rg2-rn2]
     * sn3 [rg1-rn3, rg2-rn3, rg3-rn2]
     * sn4 [rg1-rn1, rg2-rn1, rg3-rn1]
     *
     * Recall that all rg*-rn1 nodes become initial masters. So sn4 is
     * unbalanced and two masters need to be migrated off it to bring it
     * back in balance.
     */
    @Test
    public void test1234()
        throws Exception {

        initTopology(3    /* RF*/,
                     MB_PER_SN, /* MB per SN */
                     4,   /* numSNs */
                     1, 2, 3, 4 /* SN capacities */ );

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        /* Expect a balanced KVS at the end with all RNs up */
        awaitBalancedKVS();
    }

    /**
     * Starts up a 3 SN, each with capacity 3, configuration and verifies
     * that the KVS is balanced. Note that by default, rgx-rn1 nodes are
     * masters at startup (under the current deployment pattern) and the
     * balancing shifts the masters around so that each sn has a master.
     */
    @Test
    public void testBasic()
        throws Exception {

        initTopology(3 /* RF*/,
                     MB_PER_SN, /* MB per SN */
                     3, /* numSNs */
                     3, 3, 3 /* SN capacities */ );

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        /* Expect a balanced KVS at the end with all RNs up */
        awaitBalancedKVS();
        veryifyJsonTestMasterBalanceOutput();

        /* Now fail a master, so that it moves elsewhere. */

        /* Select a master on sn3. */
        final StorageNodeId sn3Id = new StorageNodeId(3);
        RepNodeId mrnId = selectMasterOnSN(sn3Id);
        assertNotNull(mrnId);
        stopMaster(mrnId);

        /*
         * Expect a balanced KVS with a master moving to sn2 or sn1 and
         * another master moving to sn3
         */
        awaitBalancedKVS();

        mrnId = selectMasterOnSN(sn3Id);
        assertNotNull(mrnId);
        stopMaster(mrnId);

        awaitBalancedKVS();
    }

    /**
     * Verifies that the isMasterBalnced attribute is present in JSON and
     * human readable output for a balanced KV
     */
    private void veryifyJsonTestMasterBalanceOutput() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        Ping.pingTopology(topo, null, false, 0, ps, null, null, logger);
        String textRep = baos.toString();
        logger.info("text rep:" + baos.toString());
        Pattern p = Pattern.compile("(.*isMasterBalanced: true){3}.*",
                                    Pattern.DOTALL);
        Matcher m = p.matcher(textRep);
        assertTrue(textRep, m.matches());

        baos = new ByteArrayOutputStream();
        ps = new PrintStream(baos);
        Ping.pingTopology(topo, null, false, CommandParser.JSON_V1, ps, null,
                          null, logger);
        textRep = baos.toString();
        p = Pattern.compile("(.*\"isMasterBalanced\": true){3}.*",
                            Pattern.DOTALL);
        m = p.matcher(textRep);
        logger.info("json rep:" + baos.toString());
        assertTrue(textRep, m.matches());
    }


    private class ExitPauseHook implements TestHook<StorageNodeAgent> {

        private final StorageNodeId snId;
        private final int sleepMs;
        volatile boolean done = false;

        ExitPauseHook(StorageNodeId snId, int sleepMs) {
            this.snId = snId;
            this.sleepMs = sleepMs;
        }

        @Override
        public void doHook(StorageNodeAgent sna) {
            if (!done && snId.equals(sna.getStorageNodeId())) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    fail("unexpected interrupt");
                } finally {
                    done = true;
                }
            }
        }
    }

    /**
     * Start 3 SNs each with capacity 3. Each SN has 1 Master.
     * 1 )Shut down SN1 first. This causes RN Master transfer from SN1 to one of
     * SN2 or SN3.
     * 2) Shut down the SN with 2 Masters in it. This cause 2 Master transfer
     * from this SN to the only SN that is up.
     * 3) From SN2 or SN3 restart that SN that is down. Master Balancing takes
     * place. SN2 and SN3 have master distribution of 66% and 33% now.
     * 4) Restart SN1. Equal master distribution takes place. Each of the SNs
     * have 1 Master.
     */
    @Test
    public void testMasterTransfersOnSNShutdownAndThenRestart()
        throws Exception {

        initTopology(3 /* RF*/,
                MB_PER_SN, /* MB per SN */
                3, /* numSNs */
                3, 3, 3 /* SN capacities */ );

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));
        final int pollPeriodMs = 1000;

        Set<Integer> targetMDs = new HashSet<Integer>();
        final StorageNodeId sn1Id = new StorageNodeId(1);
        final StorageNodeId sn2Id = new StorageNodeId(2);
        final StorageNodeId sn3Id = new StorageNodeId(3);
       /*
        * Lower the period used to check for rebalancing from 60s to
        * 1s to make the test run faster.
        */
       RebalanceThread.setPollPeriodMs(pollPeriodMs);

       /*
        * Wait for a balanced state where in all the SNs have 1 master.
        */
       targetMDs.add(33); targetMDs.add(33); targetMDs.add(33);
       awaitDefinedKVSState(targetMDs, topo.getStorageNodeIds());

       final SNSet snSet = sysAdminInfo.getSNSet();

       /*
        * Get hold of the topology caches from all the SNs
        */
       TopoCache topoCache1 = ((MasterBalanceManager)snSet.
           getMasterBalanceManager(sn1Id.getStorageNodeId() - 1))
           .getTopoCache();
       TopoCache topoCache2 = ((MasterBalanceManager)snSet.
           getMasterBalanceManager(sn2Id.getStorageNodeId() - 1))
           .getTopoCache();
       TopoCache topoCache3 = ((MasterBalanceManager)snSet.
           getMasterBalanceManager(sn3Id.getStorageNodeId() - 1))
           .getTopoCache();

       /*
        * During the initial deployment, wait till all the SNs share similar
        * topology before transferring the masters and shutting down.
        */
       while (true) {
           Topology topo1 = topoCache1.getTopology();
           Topology topo2 = topoCache2.getTopology();
           Topology topo3 = topoCache3.getTopology();
           PartitionMap pmap1 = topo1.getPartitionMap();
           PartitionMap pmap2 = topo2.getPartitionMap();
           PartitionMap pmap3 = topo3.getPartitionMap();
           Thread.sleep(1000);
           if (pmap1.getNPartitions() == 0 || pmap2.getNPartitions() == 0
               || pmap3.getNPartitions() == 0) {
               continue;
           }
           if (!pmap1.equals(pmap2) && !pmap2.equals(pmap3)) {
               continue;
           }
           break;
       }
       /*
        * Shut down SN1. This transfers 1 Master from SN1 to one of SN2 or SN3.
        */
       snSet.shutdown(sn1Id.getStorageNodeId() - 1, logger);

       /*
        * Check SN2 and SN3 have 33% (1 Master) and 66% (2 Masters) distribution
        * among themselves.
        */
       List<StorageNodeId> storageNodeIds = new LinkedList<StorageNodeId>();
       storageNodeIds.add(sn2Id); storageNodeIds.add(sn3Id);
       targetMDs.clear();
       targetMDs.add(66); targetMDs.add(33);
       awaitDefinedKVSState(targetMDs, storageNodeIds);
       logger.info("MDs after sn1 shutdown:" + getMDInfos());

       /*
        * From SN2 and SN3 get the handle on the Master Balance Manager
        * for that SN whice has 2 Masters in it.
        */
       MasterBalanceManagerInterface mgm2 = snSet.
               getMasterBalanceManager(sn2Id.getStorageNodeId() - 1);
       storageNodeIds.clear();
       if (((MasterBalanceManager)mgm2).getRebalanceThread()
           .getMasterCount() == 2) {
           /*
            * If SN2 has 2 Masters shut it down. This will transfer 2 Masters
            * from SN2 to SN3.
            */
           snSet.shutdown(sn2Id.getStorageNodeId() - 1, logger);
           storageNodeIds.add(sn3Id);
       } else {
           /*
            * If SN3 has 2 Masters shut it down. This will transfer 2 Masters
            * from SN3 to SN2.
            */
           snSet.shutdown(sn3Id.getStorageNodeId() - 1, logger);
           storageNodeIds.add(sn2Id);
       }
       logger.info("MDs after sn2 or sn3 shutdown:" + getMDInfos());
       /*
        * Check SN2 or SN3 has 100% (3 Masters)
        */
       targetMDs.clear();
       targetMDs.add(100);
       awaitDefinedKVSState(targetMDs, storageNodeIds);

       final Set<RepNodeId> rnsOnSN1 = new HashSet<RepNodeId>();
       final Set<RepNodeId> rnsOnSN2 = new HashSet<RepNodeId>();
       final Set<RepNodeId> rnsOnSN3 = new HashSet<RepNodeId>();
       for (RepNodeId rnId : topo.getRepNodeIds()) {
           if (topo.get(rnId).getStorageNodeId().equals(sn2Id)) {
               rnsOnSN2.add(rnId);
           } else if (topo.get(rnId).getStorageNodeId().equals(sn3Id)) {
               rnsOnSN3.add(rnId);
           }
       }

       /*
        * From SN2 and SN3 restart that SN that has been shut down. This will
        * migrate one Master from SN with 3 Masters to SN that has been just
        * restarted.
        */
       storageNodeIds.clear();
       storageNodeIds.add(sn2Id); storageNodeIds.add(sn3Id);
       if (((MasterBalanceManager)mgm2).getRebalanceThread()
           .getMasterCount() == 3) {
           snSet.restart(sn3Id.getStorageNodeId() - 1, rnsOnSN3, logger);
       } else {
           snSet.restart(sn2Id.getStorageNodeId() - 1, rnsOnSN2, logger);
       }

       /*
        * Check SN2 and SN3 have 33% (1 Master) and 66% (2 Masters) distribution
        * among themselves.
        */
       targetMDs.clear();
       targetMDs.add(66); targetMDs.add(33);
       awaitDefinedKVSState(targetMDs, storageNodeIds);

       for (RepNodeId rnId : topo.getRepNodeIds()) {
           if (topo.get(rnId).getStorageNodeId().equals(sn1Id)) {
               rnsOnSN1.add(rnId);
           }
       }
       /*
        * Restart SN1. This will migrate one Master from SN with 2 Masters to
        * SN1.
        */
       snSet.restart(sn1Id.getStorageNodeId() - 1, rnsOnSN1, logger);

       /*
        * Wait for a balanced state where in all the SNs have 1 master.
        */
       targetMDs.clear();
       targetMDs.add(33); targetMDs.add(33); targetMDs.add(33);
       awaitDefinedKVSState(targetMDs, topo.getStorageNodeIds());
    }

    /* Test fix for KVSTORE-1603 */
    @Test
    public void testMasterTransfersOnTransientStaleTopo()
        throws Exception {

        initTopology(3 /* RF*/,
                MB_PER_SN, /* MB per SN */
                3, /* numSNs */
                3, 3, 3 /* SN capacities */ );

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));
        final int pollPeriodMs = 1000;
        final int topocacheValidPeriodMs = 5000;

        Set<Integer> targetMDs = new HashSet<Integer>();
        final StorageNodeId sn1Id = new StorageNodeId(1);
        final StorageNodeId sn2Id = new StorageNodeId(2);
        final StorageNodeId sn3Id = new StorageNodeId(3);

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(pollPeriodMs);

        /* Reduce the validation period for the test. */
        TopoCache.setValidationIntervalMs(topocacheValidPeriodMs);

        final SNSet snSet = sysAdminInfo.getSNSet();

        /*
         * Get hold of the topology caches from all the SNs
         */
        TopoCache topoCache1 = ((MasterBalanceManager)snSet.
            getMasterBalanceManager(sn1Id.getStorageNodeId() - 1))
            .getTopoCache();
        TopoCache topoCache2 = ((MasterBalanceManager)snSet.
            getMasterBalanceManager(sn2Id.getStorageNodeId() - 1))
            .getTopoCache();
        TopoCache topoCache3 = ((MasterBalanceManager)snSet.
            getMasterBalanceManager(sn3Id.getStorageNodeId() - 1))
            .getTopoCache();

        /* force TopoCache.getRnCount() to return 0 to simulate KVSTORE-1603 */
        topoCache1.setRnCountZero = true;
        Thread.sleep(60 * 1000);

        assertEquals("sn1 rn count: ", 0, topoCache1.getRnCount());
        assertEquals("sn2 rn count: ", 3, topoCache2.getRnCount());
        assertEquals("sn3 rn count: ", 3, topoCache3.getRnCount());

        StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(sn1Id);
        assertNull("sn1 MDInfo: ", sna.getMDInfo());
        sna = regUtils.getStorageNodeAgent(sn2Id);
        assertNotNull("sn2 MDInfo: ", sna.getMDInfo());
        assertEquals("sn2 MD: ", 33, sna.getMDInfo().getMD());
        sna = regUtils.getStorageNodeAgent(sn3Id);
        assertNotNull("sn3 MDInfo: ", sna.getMDInfo());
        assertEquals("sn3 MD: ", 33, sna.getMDInfo().getMD());

        topoCache1.setRnCountZero = false;
        Thread.sleep(10 * 1000);

        assertEquals("sn1 rn count: ", 3, topoCache1.getRnCount());
        assertEquals("sn2 rn count: ", 3, topoCache2.getRnCount());
        assertEquals("sn3 rn count: ", 3, topoCache3.getRnCount());

        sna = regUtils.getStorageNodeAgent(sn1Id);
        assertNotNull("sn1 MDInfo: ", sna.getMDInfo());
        assertEquals("sn1 MD: ", 33, sna.getMDInfo().getMD());
        sna = regUtils.getStorageNodeAgent(sn2Id);
        assertNotNull("sn2 MDInfo: ", sna.getMDInfo());
        assertEquals("sn2 MD: ", 33, sna.getMDInfo().getMD());
        sna = regUtils.getStorageNodeAgent(sn3Id);
        assertNotNull("sn3 MDInfo: ", sna.getMDInfo());
        assertEquals("sn3 MD: ", 33, sna.getMDInfo().getMD());

        /*
         * Wait for a balanced state where in all the SNs have 1 master.
         */
        targetMDs.add(33); targetMDs.add(33); targetMDs.add(33);
        awaitDefinedKVSState(targetMDs, topo.getStorageNodeIds());
    }

    /**
     * This test simulates sr22737 by enlarging the timing window during
     * which an NPE may be experienced by a remote SN invoking getMDInfo on
     * an SN that is being shutdown.
     *
     * The test implements the following scenario:
     *
     * 1) Bring up a 3X3
     * 2) Wait for it to become balanced.
     * 3) Set to hook to expand the vulnerability window and ensure that the
     *  target SN (sn1) has not exited and receives the getMDInfo request.
     *  Allow enough time for: an election + 4 *  poll periodMs
     * 4) Bring down sn1 to create an imbalance, and provoke sn2 or sn3 to
     *  issue the getMDInfo() as it attempts to resolve the balance issue.
     * 5) Verify that the MBMs stay up.
     * 6) Restore sn1 and wait for the store to become balanced again.
     */
    @Test
    public void testShutdownFailure()
        throws Exception {

        initTopology(3 /* RF*/,
                     MB_PER_SN, /* MB per SN */
                     3, /* numSNs */
                     3, 3, 3 /* SN capacities */ );

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        final int pollPeriodMs = 1000;
        RebalanceThread.setPollPeriodMs(pollPeriodMs);

        /* Election time period. */
        final int electionMs = 10000;

        final StorageNodeId sn1Id = new StorageNodeId(1);

        awaitBalancedKVS();

        /* Create an imbalance. */
        final ExitPauseHook exitPauseHook =
            new ExitPauseHook(sn1Id, electionMs + 4 * pollPeriodMs);
        StorageNodeAgent.setMBMPostShutdownHook(exitPauseHook);

        final SNSet snSet = sysAdminInfo.getSNSet();
        /*
         * The -1 is needed since SNSet uses zero based sn indexes into a
         * list of sns
         */
        snSet.shutdown(sn1Id.getStorageNodeId() - 1, logger);

        assertTrue (new PollCondition(100, 60000) {

            @Override
            protected boolean condition() {
                return exitPauseHook.done;
            }
        }.await());

        StorageNodeAgent.setMBMPostShutdownHook(null);

        /*  Verify that MBMS are up by verifying */
        assertTrue(regUtils.getStorageNodeAgent(new StorageNodeId(2)).
                      getMDInfo() != null);
        assertTrue(regUtils.getStorageNodeAgent(new StorageNodeId(3)).
                    getMDInfo() != null);

        final Set<RepNodeId> rnsOnSN = new HashSet<RepNodeId>();
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            if (topo.get(rnId).getStorageNodeId().equals(sn1Id)) {
                rnsOnSN.add(rnId);
            }
        }
        snSet.restart(sn1Id.getStorageNodeId() - 1, rnsOnSN, logger);
        awaitBalancedKVS();
    }

    /**
     * Stops the master and waits for a new one to emerge in the replication
     * group.
     */
    private void stopMaster(RepNodeId mrnId)
        throws RemoteException, NotBoundException {

        final RepNode mrn = topo.get(mrnId);
        final RepGroup repGroup = topo.get(mrn.getRepGroupId());
        final StorageNodeAgentAPI sna =
            regUtils.getStorageNodeAgent(mrn.getStorageNodeId());
        sna.stopRepNode(mrnId, false, false);

        final boolean haveNewMaster = new PollCondition(1000, 30000) {

            @Override
            protected boolean condition() {
                for (RepNode rn : repGroup.getRepNodes()) {
                    if (rn.equals(mrn)) {
                        continue;
                    }

                    try {
                        RepNodeAdminAPI rna =
                            regUtils.getRepNodeAdmin(rn.getResourceId());
                        if (rna.ping().getReplicationState().isMaster()) {
                            return true;
                        }
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                }
                return false;
            }
        }.await();
        /* Wait for a new master to emerge in the group. */
        assertTrue("change master:" + mrnId, haveNewMaster);
    }

    /**
     * Returns the first master it can find on the SN, waiting until a master
     * emerges
     */
    private RepNodeId selectMasterOnSN(final StorageNodeId snId) {
        /* Hack to get around need for final for access from lambda */
        final AtomicReference<RepNodeId> mrnId = new AtomicReference<>();

        new PollCondition(10, 5_000 /* 5s Sufficient for election */ ) {

            @Override
            protected boolean condition() {
                for (RepNodeId rnId : topo.getRepNodeIds()) {
                    final RepNode rn = topo.get(rnId);
                    if (!rn.getStorageNodeId().equals(snId)) {
                        continue;
                    }

                    try {
                        final RepNodeStatus status =
                            regUtils.getRepNodeAdmin(rnId).ping();

                        if (status.getReplicationState().isMaster()) {
                            mrnId.set(rnId);
                            return true;
                        }
                    } catch (NotBoundException e) {
                        /* Some node is down. */
                        continue;
                    } catch (RemoteException e) {
                        /* Some node is down. */
                        continue;
                    }
                }
                return false;
            }
        }.await();
        return mrnId.get();
    }

    private void awaitDefinedKVSState(final Set<Integer> targetMDs,
        final List<StorageNodeId> storageNodeIds) {
        boolean ok = new PollCondition(1000, 60_000) {
                Set<Integer> checkTargetMDs = new HashSet<Integer>();

                @Override
                protected boolean condition() {
                    checkTargetMDs.addAll(targetMDs);
                    try {
                        for (StorageNodeId snId : storageNodeIds) {
                            final StorageNodeAgentAPI sna =
                                regUtils.getStorageNodeAgent(snId);
                            final MDInfo mdInfo = sna.getMDInfo();
                            logger.info("Master density for " + snId + ": " +
                                        ((mdInfo == null) ?
                                         null :
                                         mdInfo.getMD()));
                            if (mdInfo == null) {
                                continue;
                            }
                            int masterDensity = mdInfo.getMD();
                            if (checkTargetMDs.contains(masterDensity)) {
                                checkTargetMDs.remove(masterDensity);
                            }
                        }
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                    return checkTargetMDs.isEmpty();
                }

            }.await();
        assertTrue("Awaiting DefinedKVSState:" + getMDInfos(), ok);
    }

    /**
     * Wait until the KVS is balanced. That is, each SN is at the target MD.
     */
    private void awaitBalancedKVS()  {

        final boolean ok = new PollCondition(1000, 60000) {

            @Override
            protected boolean condition() {

                try {
                    for (StorageNodeId snId : topo.getStorageNodeIds()) {
                        StorageNodeAgentAPI sna =
                            regUtils.getStorageNodeAgent(snId);
                        Boolean isMasterBalanced = sna.ping().isMasterBalanced();
                        if ((isMasterBalanced == null) || !isMasterBalanced) {
                            return false;
                        }
                    }
                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return true;
            }

        }.await();

        String msg = getMDInfos();
        assertTrue(msg, ok);
    }

    /**
     * Return the mdinfo from all sns in the topology as a string.
     */
    protected String getMDInfos() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(baos);
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            try {
                StorageNodeAgentAPI sna =
                    regUtils.getStorageNodeAgent(snId);
                MDInfo mdInfo = sna.getMDInfo();
                ps.println(topo.get(snId) + " MD:" +
                          ((mdInfo == null) ? "?" : mdInfo.toString()));
            } catch (RemoteException | NotBoundException e) {
                ps.println(topo.get(snId) + " Exception:" +
                    e.getClass().getName() + " msg:" + e.getMessage());
            }

        }

        Ping.pingTopology(topo, null, false, CommandParser.JSON_V1,
                          ps, null, null, logger);
        String msg = baos.toString();
        return msg;
    }

    @Test
    public void testAffinityBasic() throws Exception {
        initMultipleDCsTopo(3,                  /* numSNs */
                            new int[]{2, 2, 2}, /* Capacities for SNs */
                            3,                  /* numDCs */
                            new int[]{1, 1, 1}, /* RF for DCs */
                            new int[]{1, 1, 1}, /* Assign SNs to DCs */

                            /* Affinity for DCs */
                            new boolean[]{false, true, false});

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        awaitAffinityBalancedKVS(new StorageNodeId(2), 2);
    }

    @Test
    public void testAffinityChanged() throws Exception {
        initMultipleDCsTopo(3,                  /* numSNs */
                            new int[]{2, 2, 2}, /* Capacities for SNs */
                            3,                  /* numDCs */
                            new int[]{1, 1, 1}, /* RF for DCs */
                            new int[]{1, 1, 1}, /* Assign SNs to DCs */

                            /* Affinity for DCs */
                            new boolean[]{false, true, true});

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        awaitAffinityBalancedKVS(new StorageNodeId(2), 1);
        awaitAffinityBalancedKVS(new StorageNodeId(3), 1);

        /* Change DC2 to no master affinity */
        changeZoneAffinityAndDeploy("DC2", false);

        /* All masters should be in SN3 */
        awaitAffinityBalancedKVS(new StorageNodeId(3), 2);
    }

    @Test
    public void testAffinityRestart() throws Exception {
        initMultipleDCsTopo(3,                  /* numSNs */
                            new int[]{2, 2, 2}, /* Capacities for SNs */
                            3,                  /* numDCs */
                            new int[]{1, 1, 1}, /* RF for DCs */
                            new int[]{1, 1, 1}, /* Assign SNs to DCs */

                            /* Affinity for DCs */
                            new boolean[]{true, true, false});

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        awaitAffinityBalancedKVS(new StorageNodeId(1), 1);
        awaitAffinityBalancedKVS(new StorageNodeId(2), 1);

        /* Shut down sn2 */
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.shutdown(1, logger);

        /* All masters should be in SN1 */
        awaitAffinityBalancedKVS(new StorageNodeId(1), 2);

        final Set<RepNodeId> rnsOnSN2 = new HashSet<RepNodeId>();
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            if (topo.get(rnId).getStorageNodeId().getStorageNodeId() == 2) {
                rnsOnSN2.add(rnId);
            }
        }

        snSet.restart(1, rnsOnSN2, logger);

        /* masters should be in SN1 and SN2 */
        awaitAffinityBalancedKVS(new StorageNodeId(1), 1);
        awaitAffinityBalancedKVS(new StorageNodeId(2), 1);
    }

    @Test
    public void testAffinityDeployMasterAffinitySecondaryZone()
            throws Exception {
        checkException(() -> initMultipleDCsTopo(
                           3,                  /* numSNs */
                           new int[]{2, 2, 2}, /* Capacities for SNs */
                           3,                  /* numDCs */
                           /* Is primaries for DCs */
                           new boolean[]{true, false, false},
                           new int[]{1, 1, 1}, /* RF for DCs */
                           new int[]{1, 1, 1}, /* Assign SNs to DCs */

                           /* Affinity for DCs */
                           new boolean[]{true, true, false}),
                       AdminFaultException.class,
                       "Master affinity is not allowed for secondary zones.");
    }

    @Test
    public void testAffinityChangeMasterAffinitySecondaryZone()
            throws Exception {
        initMultipleDCsTopo(3,                  /* numSNs */
                            new int[]{2, 2, 2}, /* Capacities for SNs */
                            3,                  /* numDCs */
                            /* Is primaries for DCs */
                            new boolean[]{true, false, false},
                            new int[]{1, 1, 1}, /* RF for DCs */
                            new int[]{1, 1, 1}, /* Assign SNs to DCs */

                            /* Affinity for DCs */
                            new boolean[]{true, false, false});
        checkException(
            /* Change DC2 to master affinity */
            () -> changeZoneAffinityAndDeploy("DC2", true),
            AdminFaultException.class,
            "Master affinity is not allowed for secondary zones.");
    }

    @Test
    public void testAffinityOnlyMasterShutdown() throws Exception {
        initMultipleDCsTopo(3,                  /* numSNs */
                            new int[]{2, 2, 2}, /* Capacities for SNs */
                            3,                  /* numDCs */
                            new int[]{1, 1, 1}, /* RF for DCs */
                            new int[]{1, 1, 1}, /* Assign SNs to DCs */

                            /* Affinity for DCs */
                            new boolean[]{false, true, false});

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        awaitAffinityBalancedKVS(new StorageNodeId(2), 2);

        /* Shut down sn2 */
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.shutdown(1, logger);
        logger.info("MDs after sn2 shutdown:" + getMDInfos());
        /* Masters should be in SN1 and SN3*/
        awaitAffinityBalancedKVS(new StorageNodeId(1), 1);
        awaitAffinityBalancedKVS(new StorageNodeId(3), 1);

        final Set<RepNodeId> rnsOnSN2 = new HashSet<RepNodeId>();
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            if (topo.get(rnId).getStorageNodeId().getStorageNodeId() == 2) {
                rnsOnSN2.add(rnId);
            }
        }

        snSet.restart(1, rnsOnSN2, logger);

        /* Masters should be in SN2 */
        awaitAffinityBalancedKVS(new StorageNodeId(2), 2);
    }

    @Test
    public void testMasterBalanceSecondaryZone() throws Exception {
        initMultipleDCsTopo(5,                  /* numSNs */
                            new int[]{3, 1, 1, 1, 3}, /* Capacities for SNs */
                            3,                  /* numDCs */
                            /* Is primaries for DCs */
                            new boolean[]{true, true, false},
                            new int[]{1, 1, 1}, /* RF for DCs */
                            new int[]{1, 3, 1}, /* Assign SNs to DCs */

                            /* Affinity for DCs */
                            new boolean[]{false, false, false});

        logger.info("deployed:" + TopologyPrinter.printTopology(topo));

        /*
         * Lower the period used to check for rebalancing from 60s to
         * 1s to make the test run faster.
         */
        RebalanceThread.setPollPeriodMs(1000);

        /* Expect a balanced KVS at the end with all RNs up */
        awaitBalancedKVS();

        /*
         * Simulate killing (stop and start) a master RN on SN2 or SN3
         * or SN4 (dc2).
         */
        StorageNodeId snId = null;
        RepNodeId mrnId = null;
        for (int i = 2; i < 5; i++) {
            snId = new StorageNodeId(i);
            mrnId = selectMasterOnSN(snId);
            if(mrnId != null) {
               break;
            }
        }

        assertNotNull(mrnId);
        assertNotNull(snId);
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
        assertNotNull(sna);
        sna.stopRepNode(mrnId, false, false);
        sna.startRepNode(mrnId);

        /*
         * Expect a balanced KVS with a master moving to SN1 (dc1) and
         * another master moving to SN2 or SN3 or SN4 (dc2).
         */
        awaitBalancedKVS();
        snId = new StorageNodeId(1);

        MDInfo mdInfo1 = regUtils.getStorageNodeAgent(snId).getMDInfo();
        assertEquals(66, mdInfo1.getBMD());
        assertTrue(mdInfo1.getMD() <= 66);
    }
}
