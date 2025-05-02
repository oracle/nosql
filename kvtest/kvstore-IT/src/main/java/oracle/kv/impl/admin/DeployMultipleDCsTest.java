/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test deploying topologies with multiple data centers.
 */
public class DeployMultipleDCsTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

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
    }

    /**
     * Deploy a topology with 1 shard in two data centers:
     *
     * - DC1: RF=2, 3 SNs (1 overcapacity)
     * - DC2: RF=1, 2 SNs (1 overcapacity)
     */
    @Test
    public void testTwoDCs()
        throws Exception {

        /* Create DC1 */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 5 /* maxNumSNs */, "DC1", 2 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        /* Deploy three SNs in DC1 */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet sns = sysAdminInfo.getSNSet();
        sns.deploy(cs, sysAdminInfo.getDCId("DC1"), MB_PER_SN_STRING, 0, 1, 2);

        /* Create DC2 */
        AdminUtils.deployDatacenter(cs, "DC2", 1 /* repFactor */, sysAdminInfo);

        /* Deploy two SNs in DC2 */
        sns.deploy(cs, sysAdminInfo.getDCId("DC2"), MB_PER_SN_STRING, 3, 4);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        final int planNum =
            cs.createDeployTopologyPlan("DeployTopology", "TopoA", null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(), 2 /* DCs */, 5 /* SNs */,
                              1 /* shards */, 3 /* RNs */,
                              100 /* partitions */);
    }

    /**
     * Test deploying a topology with 3 shards, rep factor 4, in three data
     * centers:
     *
     * - DC1: RF=2, 2 SNs with capacity 3
     * - DC2: RF=1, 2 SNs with capacity 2 (overcapacity)
     * - DC3: RF=1, 3 SNs with capacity 1
     */
    @Test
    public void testThreeDCs()
        throws Exception {

        /* Create DC1 with RF=2 */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 7 /* maxNumSNs */, "DC1", 2 /* repFactor*/,
            true /* useThreads*/, MB_PER_SN_STRING /* memoryMB */);

        /* Deploy two SNs in DC1 with capacity three */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet sns = sysAdminInfo.getSNSet();
        sns.deploy(cs, sysAdminInfo.getDCId("DC1"), MB_PER_SN_STRING, 0, 1);
        sns.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1);

        /* Create DC2 with RF=1 */
        AdminUtils.deployDatacenter(cs, "DC2", 1 /* repFactor */, sysAdminInfo);

        /* Deploy two SNs in DC2 with capacity two */
        sns.deploy(cs, sysAdminInfo.getDCId("DC2"), MB_PER_SN_STRING, 2, 3);
        sns.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 2, 3);

        /* Create DC3 with RF=1 */
        AdminUtils.deployDatacenter(cs, "DC3", 1 /* repFactor */, sysAdminInfo);

        /* Deploy 3 SNs in DC3 */
        sns.deploy(cs, sysAdminInfo.getDCId("DC3"), MB_PER_SN_STRING, 4, 5, 6);

        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        final int planNum =
            cs.createDeployTopologyPlan("DeployTopology", "TopoA", null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(), 3 /* DCs */, 7 /* SNs */,
                              3 /* shards */, 12 /* RNs */,
                              100 /* partitions */);
    }
}
