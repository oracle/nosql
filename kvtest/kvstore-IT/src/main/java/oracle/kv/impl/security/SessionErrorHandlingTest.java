/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Test SessionAccessException handling in Remote APIs.
 */
public class SessionErrorHandlingTest extends TestBase {
    @Override
    public void setUp() throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();

        /* revert test hook */
        ContextProxy.beforeInvokeNoAuthRetry = null;
    }

    @Test
    public void testVerifyConfig()
        throws Exception {

        PortFinder portFinder = new PortFinder(13270, 5);
        AdminTestConfig atc = new AdminTestConfig(kvstoreName, portFinder);
        AdminServiceParams adminServiceParams = atc.getParams();
        Admin admin = new Admin(adminServiceParams);
        StorageNodeAgent sna1 = StorageNodeTestBase.createUnregisteredSNA(
            portFinder, 1, "config0.xml", false, true, false, MB_PER_SN);
        tearDowns.add(() -> {sna1.shutdown(true, true);});

        int planId = admin.getPlanner().createDeployDatacenterPlan(
            "deploy data center", "DC1", 1 /* repFactor */,
            DatacenterType.PRIMARY, false, false);
        runPlan(admin, planId);

        StorageNodeParams snParams = new StorageNodeParams(
            "localhost", portFinder.getRegistryPort(), "sn1");
        planId = admin.getPlanner().createDeploySNPlan("deploy storage node",
            new DatacenterId(1), snParams);
        runPlan(admin, planId);

        planId = admin.getPlanner().
            createDeployAdminPlan("deploy admin", new StorageNodeId(1));
        runPlan(admin, planId);

        admin.createTopoCandidate("topo", Parameters.DEFAULT_POOL_NAME,
            10, false, SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        planId = admin.getPlanner().createDeployTopoPlan(
            "firstPlan", "topo", null);
        runPlan(admin, planId);

        /*
         * VerifyConfiguration verify call can cover various remote APIs,
         * e.g. CommandServiceAPI, RepNodeAdminAPI and StorageNodeAgentAPI.
         */
        VerifyConfiguration vc =
            new VerifyConfiguration(admin, true, true, false, logger);

        /* inject SAE */
        ContextProxy.beforeInvokeNoAuthRetry = new TestUtils
            .CountDownFaultHook(10 /* fault count */,
            new SessionAccessException("sae"));
        verifyFailed(vc);

        /* inject FaultException caused by SAE */
        ContextProxy.beforeInvokeNoAuthRetry = new TestUtils
            .CountDownFaultHook(10 /* fault count */,
            new SNAFaultException(new SessionAccessException("sae")));
        verifyFailed(vc);
    }

    private void verifyFailed(VerifyConfiguration vc) {
        assertFalse(vc.verifyTopology());
        VerifyResults results = vc.getResults();
        List <Problem> violations = results.getViolations();
        for (Problem p : violations) {
            assertTrue(p instanceof RMIFailed);
        }
    }

    private void runPlan(Admin admin, int planId) {
        Plan plan = admin.getPlanById(planId);
        assertEquals(plan.getId(), planId);

        admin.approvePlan(plan.getId());
        admin.savePlan(plan, Admin.CAUSE_APPROVE);
        admin.executePlan(planId, true /* force */);
        admin.awaitPlan(planId, 0, null);

        assertEquals(Plan.State.SUCCEEDED, plan.getState());
    }
}
