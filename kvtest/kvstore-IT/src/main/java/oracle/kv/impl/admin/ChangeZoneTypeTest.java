/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test deploying topologies with multiple data centers.
 */
public class ChangeZoneTypeTest extends TestBase {

    private final String ZONE1 = "Zone1";
    private final String ZONE2 = "Zone2";
    private final String ZONE3 = "Zone3";
    private final String ZONE4 = "Zone4";

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;
    private String savedRunVerifier = null;

    @Override
    public void setUp()
        throws Exception {

        /* Allow write-no-sync durability for faster test runs. */
        TestStatus.setWriteNoSyncAllowed(true);
        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (su != null) {
            su.close();
        }
        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
        }
        LoggerUtils.closeAllHandlers();
        if (savedRunVerifier != null) {
            System.setProperty("test.je.env.runVerifier", savedRunVerifier);
            savedRunVerifier = null;
        }
        // oracle.kv.impl.async.AsyncTestUtils.checkActiveDialogTypes();
    }

    /*
     * Tests changing a zone from a secondary to a primary
     */
    @Test
    public void testChangeZoneToPrimary()
        throws Exception {

        /* Create zone 1 */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 5 /* maxNumSNs */, ZONE1, 2 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        /* Deploy three SNs in zone1 */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING,
                     0, 1, 2);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Create zone2 as a secondary */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);

        /* Deploy two SNs in zone2 */
        final DatacenterId zone2Id = sysAdminInfo.getDCId(ZONE2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 3, 4);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        cs.copyCurrentTopology("TopoB");
        cs.changeZoneType("TopoB", zone2Id, DatacenterType.PRIMARY);

        planNum = cs.createDeployTopologyPlan("Change Zone2 Type", "TopoB",
                                              null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        awaitVerification(cs);
    }

    /*
     * Tests changing a zone from a primary to a secondary
     */
    @Test
    public void testChangeZoneToSecondary()
        throws Exception {

        /* Create zone 1 */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 5 /* maxNumSNs */, ZONE1, 2 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        /* Deploy three SNs in zone1 */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING,
                     0, 1, 2);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Create zone2 as a primary */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        /* Deploy two SNs in zone2 */
        final DatacenterId zone2Id = sysAdminInfo.getDCId(ZONE2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 3, 4);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        cs.copyCurrentTopology("TopoB");
        cs.changeZoneType("TopoB", zone2Id, DatacenterType.SECONDARY);

        final AdminFaultException afe = checkException(
            () -> cs.createDeployTopologyPlan(
                "Change Zone2 Type", "TopoB", null),
            AdminFaultException.class,
            "Attempt to reduce the overall primary replication factor by" +
            " 1 from 3 to 2.");
        assertEquals(IllegalCommandException.class.getName(),
                     afe.getFaultClassName());

        awaitVerification(cs);
    }

    /*
     * Tests changing the primary zone containing the admin master from a
     * primary to a secondary when there are just two primary zones.
     */
    @Test
    public void testChangeZoneToSecondaryRepFactorTwo()
        throws Exception {

        /*
         * Disable JE data verification to see if that is part of what is
         * causing persistent occasional failures of this test.
         * TODO: Consider removing this change
         */
        savedRunVerifier = System.getProperty("test.je.env.runVerifier");
        System.setProperty("test.je.env.runVerifier", "false");

        /*
         * Create zone 1.  Use processes, not threads, so that the SNA can
         * restart admins as needed by this test.
         */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 6 /* maxNumSNs */, ZONE1, 1 /* repFactor*/,
            false /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        /* Deploy two SNs in zone1 */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        final DatacenterId zone1Id = sysAdminInfo.getDCId(ZONE1);
        snSet.deploy(cs, zone1Id, MB_PER_SN_STRING, 0, 1);

        /* Create zone2 as a primary */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        /* Deploy two SNs in zone2 */
        final DatacenterId zone2Id = sysAdminInfo.getDCId(ZONE2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 2, 3);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Deploy two SNs in zone3 as a secondary zone */
        AdminUtils.deployDatacenter(cs, ZONE3, 1 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);
        final DatacenterId zone3Id = sysAdminInfo.getDCId(ZONE3);
        snSet.deploy(cs, zone3Id, MB_PER_SN_STRING, 4, 5);
        AdminUtils.deployAdmin(cs, snSet, 4);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        cs.copyCurrentTopology("TopoB");
        cs.changeZoneType("TopoB", zone1Id, DatacenterType.SECONDARY);
        cs.changeZoneType("TopoB", zone3Id, DatacenterType.PRIMARY);

        planNum = cs.createDeployTopologyPlan("Change Zone1 Type", "TopoB",
                                              null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        try {
            cs.awaitPlan(planNum, 0, null);
            fail("Expected AdminFaultException(AdminNotReadyException)");
        } catch (AdminFaultException afe) {
            if (!afe.getFaultClassName().equals(
                                      AdminNotReadyException.class.getName())) {
                fail("Expected AdminFaultException(AdminNotReadyException)");
            }

            final AtomicReference<CommandServiceAPI> newCs =
                new AtomicReference<>();
            final CommandServiceAPI cs2 = StorageNodeUtils.waitForAdmin(
                AdminUtils.HOSTNAME, snSet.getRegistryPort(2), 30);
            final CommandServiceAPI cs3 = StorageNodeUtils.waitForAdmin(
                AdminUtils.HOSTNAME, snSet.getRegistryPort(3), 30);
            assertTrue("Waiting for master",
                       new PollCondition(1000, 90000) {
                           @Override
                           protected boolean condition() {
                               try {
                                   if (cs2.getAdminStatus()
                                       .getIsAuthoritativeMaster()) {
                                       newCs.set(cs2);
                                       return true;
                                   }
                               } catch (Exception e) {
                               }
                               try {
                                   if (cs3.getAdminStatus()
                                       .getIsAuthoritativeMaster()) {
                                       newCs.set(cs3);
                                       return true;
                                   }
                               } catch (Exception e) {
                               }
                               return false;
                           }
                       }.await());
            cs = newCs.get();
        }

        /* And wait for the original admin to restart */
        StorageNodeUtils.waitForAdmin(
            AdminUtils.HOSTNAME, snSet.getRegistryPort(1), 30);

        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        awaitVerification(cs);
    }

    /*
     * Tests attempting to deploy a topo with no primary zones.
     */
    @Test
    public void testChangeToNoPrimary()
        throws Exception {

        /* Create zone 1 */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 5 /* maxNumSNs */, ZONE1, 2 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING,
                     0, 1, 2);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Create zone2 as a secondary */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);

        final DatacenterId zone2Id = sysAdminInfo.getDCId(ZONE2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 3, 4);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        cs.copyCurrentTopology("TopoB");
        /* Change zone 1 to be a secondary as well */
        cs.changeZoneType("TopoB", sysAdminInfo.getDCId(ZONE1),
                          DatacenterType.SECONDARY);

        final AdminFaultException afe =
            checkException(
                () -> cs.createDeployTopologyPlan("No Primary", "TopoB", null),
                AdminFaultException.class,
                "Attempt to reduce the overall primary replication factor" +
                " by 2 from 2 to 0.");
        assertEquals(IllegalCommandException.class.getName(),
                     afe.getFaultClassName());

        /* Nothing should have changed */
        awaitVerification(cs);
    }

    /*
     * Tests the order of chaning multiple zones.
     * Three zones with the following RF:
     *   Zone 1: RF=1 Primary
     *   Zone 2: RF=2 Primary
     *   Zone 3: RF=2 Secondary
     * Then swap zone 2 and 3. If they are done of of order or we lose zone 2,
     * the Admin will lose quorum and the plan/test will fail.
     */
    @Test
    public void testSwapZones()
        throws Exception {

        /* Create zone 1 */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 6 /* maxNumSNs */, ZONE1, 1 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        /* Deploy three SNs in zone1 */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING, 0);

        /* Create zone2 as a primary */
        AdminUtils.deployDatacenter(cs, ZONE2, 2 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone2Id = sysAdminInfo.getDCId(ZONE2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 2, 3);
        AdminUtils.deployAdmin(cs, snSet, 2);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Create zone3 as a secondary */
        AdminUtils.deployDatacenter(cs, ZONE3, 2 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);

        final DatacenterId zone3Id = sysAdminInfo.getDCId(ZONE3);
        snSet.deploy(cs, zone3Id, MB_PER_SN_STRING, 4, 5);
        AdminUtils.deployAdmin(cs, snSet, 4);
        AdminUtils.deployAdmin(cs, snSet, 5);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        cs.copyCurrentTopology("TopoB");
        /*
         * Swap the types of zone 2 and 3. If this is done in the wrong order
         * the Admin will loose quorum and the plan will fail.
         */
        cs.changeZoneType("TopoB", zone2Id, DatacenterType.SECONDARY);
        cs.changeZoneType("TopoB", zone3Id, DatacenterType.PRIMARY);

        planNum = cs.createDeployTopologyPlan("Swap Zone Type", "TopoB", null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        awaitVerification(cs);
    }

    /*
     * Tests the case where a down RN would cause a shard to lose
     * quorum after a change zone type operation.
     */
    @Test
    public void testLossOfRNQuorum()
        throws Exception {

        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 4 /* maxNumSNs */, ZONE1, 1 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING, 0);

        /* Create zone2 */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone2Id = new DatacenterId(2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 1);
        AdminUtils.deployAdmin(cs, snSet, 1);

        /* Create zone3 */
        AdminUtils.deployDatacenter(cs, ZONE3, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone3Id = sysAdminInfo.getDCId(ZONE3);
        snSet.deploy(cs, zone3Id, MB_PER_SN_STRING, 2);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Create zone4 as a secondary zone */
        AdminUtils.deployDatacenter(cs, ZONE4, 1 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);
        final DatacenterId zone4Id = sysAdminInfo.getDCId(ZONE4);
        snSet.deploy(cs, zone4Id, MB_PER_SN_STRING, 3);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Shorten the wait time out for health check, but not too short so
         * that other operations can succeed
         */
        AdminUtils.changeAllAdminParams(
                cs, ParameterState.AP_WAIT_TIMEOUT, "1 minutes");

        cs.copyCurrentTopology("TopoB");

        /* Stop a RN in zone3, This leaves only two primary RNs running. */
        snSet.getSNA(2).stopRepNode(new RepNodeId(1, 3), true);

        /*
         * Change Zone 2 to a secondary and Zone 4 to primary, which converts
         * a primary admin to a secondary one, but leaves the overall primary
         * RF the same
         */
        cs.changeZoneType("TopoB", zone2Id, DatacenterType.SECONDARY);
        cs.changeZoneType("TopoB", zone4Id, DatacenterType.PRIMARY);

        final int changeTypePlanNum =
            cs.createDeployTopologyPlan("Change Zone Type", "TopoB", null);
        cs.approvePlan(changeTypePlanNum);
        checkException(
            () -> {
                cs.executePlan(changeTypePlanNum, false);
                cs.awaitPlan(changeTypePlanNum, 0, null);
                cs.assertSuccess(changeTypePlanNum);
            },
            AdminFaultException.class,
            "One of the groups is not healthy enough");
    }

    /*
     * Tests the case where a down Admin would cause a shard to lose
     * quorum after a change zone type operation.
     */
    @Test
    public void testLossOfAdminQuorum()
        throws Exception {

        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 4 /* maxNumSNs */, ZONE1, 1 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING, 0);

        /* Create zone2 */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone2Id = new DatacenterId(2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 1);
        AdminUtils.deployAdmin(cs, snSet, 1);

        /* Create zone3 */
        AdminUtils.deployDatacenter(cs, ZONE3, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone3Id = sysAdminInfo.getDCId(ZONE3);
        snSet.deploy(cs, zone3Id, MB_PER_SN_STRING, 2);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Create zone4 as a secondary zone */
        AdminUtils.deployDatacenter(cs, ZONE4, 1 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);
        final DatacenterId zone4Id = sysAdminInfo.getDCId(ZONE4);
        snSet.deploy(cs, zone4Id, MB_PER_SN_STRING, 3);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Reduce the wait timeout so that the lack of admin quorum later is
         * detected more quickly. But don't make this too short, since other
         * operations need to succeed.
         */
        AdminUtils.changeAllAdminParams(
            cs, ParameterState.AP_WAIT_TIMEOUT, "1 minutes");

        cs.copyCurrentTopology("TopoB");

        /*
         * Stop the Admin in zone3, This leaves only two primary admins
         * running.
         */
        RegistryUtils ru = new RegistryUtils(cs.getTopology(), null, logger);
        StorageNodeAgentAPI snai = ru.getStorageNodeAgent(new StorageNodeId(3));
        assertNotNull(snai);
        assertTrue(snai.stopAdmin(false));

        /*
         * Change Zone 2 to a secondary and Zone 4 to primary, which converts
         * a primary admin to a secondary one, but leaves the overall primary
         * RF the same
         */
        cs.changeZoneType("TopoB", zone2Id, DatacenterType.SECONDARY);
        cs.changeZoneType("TopoB", zone4Id, DatacenterType.PRIMARY);

        final int changeTypePlanNum =
            cs.createDeployTopologyPlan("Change Zone Type", "TopoB", null);
        cs.approvePlan(changeTypePlanNum);
        cs.executePlan(changeTypePlanNum, false);
        cs.awaitPlan(changeTypePlanNum, 0, null);
        checkException(() -> cs.assertSuccess(changeTypePlanNum),
                       AdminFaultException.class,
                       "One of the groups is not healthy enough");
    }

    /*
     * Tests the case where a change zone type operation cannot be executed
     * where a shard does not have quorum or a majority of nodes in the zone
     * being changed are offline.
     */
    @Test
    public void testChangeZoneShardPreCheck()
        throws Exception {

        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 4 /* maxNumSNs */, ZONE1, 1 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(ZONE1), MB_PER_SN_STRING, 0);

        /* Create zone2 */
        AdminUtils.deployDatacenter(cs, ZONE2, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone2Id = new DatacenterId(2);
        snSet.deploy(cs, zone2Id, MB_PER_SN_STRING, 1);
        AdminUtils.deployAdmin(cs, snSet, 1);

        /* Create zone3 */
        AdminUtils.deployDatacenter(cs, ZONE3, 1 /* repFactor */,
                                    DatacenterType.PRIMARY, sysAdminInfo);

        final DatacenterId zone3Id = sysAdminInfo.getDCId(ZONE3);
        snSet.deploy(cs, zone3Id, MB_PER_SN_STRING, 2);
        AdminUtils.deployAdmin(cs, snSet, 2);

        /* Create zone4 as a secondary zone */
        AdminUtils.deployDatacenter(cs, ZONE4, 1 /* repFactor */,
                                    DatacenterType.SECONDARY, sysAdminInfo);
        final DatacenterId zone4Id = sysAdminInfo.getDCId(ZONE4);
        snSet.deploy(cs, zone4Id, MB_PER_SN_STRING, 3);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("Deploy Topology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        cs.copyCurrentTopology("TopoB");

        /* Stop a RN in zone1 and zone3. This leaves the shard lose quorum. */
        RepNodeId rg1rn1 = new RepNodeId(1, 1);
        RepNodeId rg1rn3 = new RepNodeId(1, 3);
        snSet.getSNA(0).stopRepNode(rg1rn1, true);
        snSet.getSNA(2).stopRepNode(rg1rn3, true);

        /*
         * Change Zone 2 to a secondary and Zone 4 to primary, to leave the
         * overall primary RF the same
         */
        cs.changeZoneType("TopoB", zone2Id, DatacenterType.SECONDARY);
        cs.changeZoneType("TopoB", zone4Id, DatacenterType.PRIMARY);

        final int changeTypePlanNum =
            cs.createDeployTopologyPlan("Chnage Zone Type", "TopoB", null);
        cs.approvePlan(changeTypePlanNum);
        checkException(() -> cs.executePlan(changeTypePlanNum, false),
                       AdminFaultException.class,
                       "a simple majority cannot be formed");

        /*
         * Restart RN in zone1 and stop RN in zone2. This leaves a majority of
         * nodes in zone being changed are offline.
         */
        snSet.getSNA(0).startRepNode(rg1rn1);
        StorageNodeUtils.waitForRNAdmin(rg1rn1, snSet.getId(0),
                                        kvstoreName, AdminUtils.HOSTNAME,
                                        snSet.getRegistryPort(0), 30000);
        snSet.getSNA(1).stopRepNode(new RepNodeId(1, 2), true);
        checkException(() -> cs.executePlan(changeTypePlanNum, false),
                       AdminFaultException.class,
                       "a majority of nodes");
    }

    private void awaitVerification(final CommandServiceAPI cs) {
        final AtomicReference<List<?>> violations = new AtomicReference<>();
        boolean result = new PollCondition(1000, 10000) {
            @Override
            protected boolean condition() {
                final List<?> vResult;
                try {
                    vResult = cs.verifyConfiguration(false, true, false)
                        .getViolations();
                } catch (RemoteException re) {
                    fail("Unexpected exception " + re);
                    return false;   /* not reached */
                }
                violations.set(vResult);
                return vResult.isEmpty();
            }
        }.await();
        assertTrue("Violations exist: " + violations, result);
    }
}
