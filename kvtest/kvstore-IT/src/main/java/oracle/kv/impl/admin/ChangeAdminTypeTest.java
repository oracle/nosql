/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.Validations.WrongAdminType;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test changing Admin type.
 */
public class ChangeAdminTypeTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    public ChangeAdminTypeTest() {
    }

    @Override
    public void setUp()
        throws Exception {

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
     * Test repairing a secondary Admin which is in a primary zone.
     */
    @Test
    public void testRepairSecondary() throws Exception {

        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 5 /* max SNs */,
                                                 "ZoneA",
                                                 3 /* repFactor*/);
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("ZoneA"), MB_PER_SN_STRING,
                     1, 2, 3);

        /* Delay deploying admin 3 so we can give it a different type */
        AdminUtils.deployAdmin(cs, snSet, 2);

        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("DeployTopology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Deploy admin 3 as a secondary after creating the topology, because
         * deploying a topology will correct this error
         */
        AdminUtils.deployAdmin(cs, snSet, 3, AdminType.SECONDARY);

        /* There should be one Admin which is a SECONDARY */
        VerifyResults vResult = cs.verifyConfiguration(false, true, false);
        List<VerifyConfiguration.Problem> violations = vResult.getViolations();
        assertEquals("Violations exist: " + violations, 1, violations.size());
        assertTrue(violations.get(0).getClass().equals(WrongAdminType.class));

        final AdminId secondaryId = (AdminId)violations.get(0).getResourceId();

        /* Change the SECONDARY Admin to a PRIMARY */
        planNum = cs.createRepairPlan("ChangeAdminType");
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        Parameters parameters = cs.getParameters();
        StorageNodeId snId = parameters.get(secondaryId).getStorageNodeId();
        StorageNodeParams snp = parameters.get(snId);

        StorageNodeUtils.waitForAdmin(snp.getHostname(), snp.getRegistryPort());

        vResult = cs.verifyConfiguration(false, true, false);
        violations = vResult.getViolations();
        assertEquals("Violations exist: " + violations, 0, violations.size());
    }

    /**
     * Test repairing a primary Admin which is in a secondary zone.
     */
    @Test
    public void testRepairPrimary() throws Exception {

        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 5 /* max SNs */,
                                                 "ZoneA",
                                                 3 /* repFactor*/);
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("ZoneA"), MB_PER_SN_STRING,
                     1, 2, 3);

        /* Deploy the proper # of Admins in the PRIMARY zone */
        AdminUtils.deployAdmin(cs, snSet, 2);
        AdminUtils.deployAdmin(cs, snSet, 3);

        /* Create a SECONDARY zone */
        AdminUtils.deployDatacenter(cs, "ZoneB", 1 /* RF */,
                                    DatacenterType.SECONDARY, sysAdminInfo);
        snSet.deploy(cs, sysAdminInfo.getDCId("ZoneB"), MB_PER_SN_STRING, 4);

        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("DeployTopology", "TopoA",
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /* Deploy the fourth admin but make it a primary */
        AdminUtils.deployAdmin(cs, snSet, 4, AdminType.PRIMARY);

        /* There should be one Admin which is incorrectly a PRIMARY */
        VerifyResults vResult = cs.verifyConfiguration(false, true, false);
        List<VerifyConfiguration.Problem> violations = vResult.getViolations();
        assertEquals("Violations exist: " + violations, 1, violations.size());
        assertTrue(violations.get(0).getClass().equals(WrongAdminType.class));

        final AdminId secondaryId = (AdminId)violations.get(0).getResourceId();

        /* Change the PRIMARY Admin to a SECONDARY */
        planNum = cs.createRepairPlan("ChangeAdminType");
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        Parameters parameters = cs.getParameters();
        StorageNodeId snId = parameters.get(secondaryId).getStorageNodeId();
        StorageNodeParams snp = parameters.get(snId);

        StorageNodeUtils.waitForAdmin(snp.getHostname(), snp.getRegistryPort());

        vResult = cs.verifyConfiguration(false, true, false);
        violations = vResult.getViolations();
        assertEquals("Violations exist: " + violations, 0, violations.size());
    }
}
