/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.GroupNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Tests for JVM parameter override.
 */
public class ParamOverrideTest extends TestBase {


    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    public ParamOverrideTest() {
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

    /*
     * Test jvm parameter override for Admin. This test requires at least 
     * RF 3.
     */
    @Test
    public void testAdminJVMParameterOverride()
        throws Exception {
        String dcName = "DataCenterAva";
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      9 /* SNs */,
                                      dcName,
                                      3 /* repFactor*/,
                                      false /* useThreads */,
                                      String.valueOf(
                                          MB_PER_SN) /* memory */,
                                      true /* allowArbiters */);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName),
                     String.valueOf(MB_PER_SN), 1, 2, 3, 4, 5, 6, 7, 8);
        AdminUtils.deployAdmin(cs, snSet, 2, AdminType.PRIMARY);
        AdminUtils.deployAdmin(cs, snSet, 3, AdminType.PRIMARY);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 9 /*nSNs*/, 3 /*nRG*/,
                              9/*nRN*/, 0/*nARBs*/,100);

        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                        null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        changeJVMParameter(cs, new AdminId(2));
    }

    /*
     * Test jvm parameter override for RN and Arbiter.
     */
    @Test
    public void testJVMParameterOverride()
        throws Exception {
        String dcName = "DataCenterAva";
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      6 /* SNs */,
                                      dcName,
                                      2 /* repFactor*/,
                                      false /* useThreads */,
                                      String.valueOf(
                                          2 * MB_PER_SN) /* memory */,
                                      true /* allowArbiters */);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName),
                     String.valueOf(2 * MB_PER_SN), 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);
        AdminUtils.deployAdmin(cs, snSet, 2, AdminType.PRIMARY);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 3 /*nSNs*/, 3 /*nRG*/,
                              6/*nRN*/, 3/*nARBs*/,100);

        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                        null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              3 /*numSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                              3/*nARBs*/, 100 /*numParts*/);

        changeJVMParameter(cs, new ArbNodeId(1,1));
        changeJVMParameter(cs, new RepNodeId(1,1));

        DatacenterId dcId = sysAdminInfo.getDCId(dcName);

        /*
         * test Change All Parameters Plan, setting JVM_MISC
         * and JVM_RN_OVERRIDE
        */
        changeJVMParameterAllRns(cs, dcId, ParameterState.JVM_MISC,
                "-XX:ParallelGCThreads=4", "-XX:ParallelGCThreads=4");
        changeJVMParameterAllRns(cs, dcId, ParameterState.JVM_RN_OVERRIDE,
                "-XX:ParallelGCThreads=4", "-XX:ParallelGCThreads=4");
        /* check GroupNodeParams.getMaxHeapMB() */
        changeJVMParameterAllRns(cs, dcId, ParameterState.JVM_MISC,
                "-XX:ParallelGCThreads=4 -XX:ConcGCThreads=4 " +
                        "-Xms11253M -Xmx11253M", "-Xmx11253M");
        /* check GroupNodeParams.getMinHeapMB() */
        changeJVMParameterAllRns(cs, dcId, ParameterState.JVM_RN_OVERRIDE,
                "-Xmx11253M -Xms11253M", "Xms11253M");
    }


    /*
     * Runs a change-parameters plan on all RNs in a datastore.
     * It overrides the JVM parameters for each RN. Then it checks that the
     * correct values were used for the new parameters.
     */
    private void changeJVMParameterAllRns(CommandServiceAPI cs,
                                          DatacenterId dcId,
                                          String paramName, String params,
                                          String searchWord)
            throws Exception {

        /* Set up a change-parameters plan to override JVM parameters
         * and execute plan */
        ParameterMap map = new ParameterMap();
        map.setParameter(paramName, params);
        int planId = cs.createChangeAllParamsPlan(
                "setJVMOverride", dcId, map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Create File object to read SN log results */
        File snLog =
                new File(TestUtils.getTestDir().toString() + File.separator +
                        kvstoreName + File.separator +
                        "log" + File.separator +
                        sysAdminInfo.getSNSet().getId(1) + "_0.log");

        /* verify RN parameter changes by checking SN log */
        checkLogFile(snLog, searchWord);
    }

    /*
     * executes change-parameters plan on a single node and verifies
     * correct node startup
     */
    private void changeJVMParameter(CommandServiceAPI cs, ResourceId resId)
        throws Exception {
        StorageNodeId snId = null;
        String overParam = null;
        String verifyValues[] = new String[0];
        String pName = "";

        Topology topo = cs.getTopology();
        if (resId instanceof ArbNodeId) {
            ArbNode an = topo.get((ArbNodeId)resId);
            snId = an.getStorageNodeId();
            overParam = "-Xmx77M";
            verifyValues = new String[1];
            verifyValues[0] = "-Xmx77M";
            pName = ParameterState.JVM_AN_OVERRIDE;
        } else if (resId instanceof RepNodeId) {
            RepNode rn = topo.get((RepNodeId)resId);
            snId = rn.getStorageNodeId();
            overParam = "-Xms177M -Xmx177M -XX:ParallelGCThreads=2 "+
                        "-XX:ConcGCThreads=2 " +
                        " -Dwayne=racer";
            verifyValues = new String[5];
            verifyValues[0] = "-Xmx177M";
            verifyValues[1] = "-Xms177M";
            verifyValues[2] = "-XX:ParallelGCThreads=2";
            verifyValues[3] = "-XX:ConcGCThreads=2";
            verifyValues[4] = "-Dwayne=racer";
            pName = ParameterState.JVM_RN_OVERRIDE;
        } else if (resId instanceof AdminId) {
            /* Admin sn is hard coded in this test */
            snId = new StorageNodeId(3);
            overParam = "-Dwayne=racer";
            verifyValues = new String[1];
            verifyValues[0] = "-Dwayne=racer";
            pName = ParameterState.JVM_ADMIN_OVERRIDE;
        }
        ParameterMap map = new ParameterMap();
        /* Set up a JVM override param. */
        map.setParameter(pName, overParam);
        int planId = cs.createChangeParamsPlan
            ("setJVMOverride", resId, map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        File snLog =
            new File(TestUtils.getTestDir().toString() + File.separator +
                     kvstoreName + File.separator +
                     "log" + File.separator + snId + "_0.log");

        /* Check that all parameters are used on node startup */
        for (String param : verifyValues) {
            checkLogFile(snLog, param);
        }
    }
    
    private String getReplacedPrefix(String val) {
        if (val.startsWith(GroupNodeParams.PARALLEL_GC_FLAG)) {
            return GroupNodeParams.PARALLEL_GC_FLAG;
        } 
        if (val.startsWith(GroupNodeParams.XMS_FLAG)) {
            return GroupNodeParams.XMS_FLAG;
        }
        if (val.startsWith(GroupNodeParams.XMX_FLAG)) {
            return GroupNodeParams.XMX_FLAG;
        }
        return null;
    }

    /*
     * checkLogFile looks at an RN's corresponding SN log to see if the
     * service was restarted with the correct parameter. It looks at the
     * command line for how the service was started and checks if it contains
     * the parameter "lookFor".
     */
    private void checkLogFile(File logf, String lookFor) throws Exception  {
        BufferedReader in = new BufferedReader(new FileReader(logf));
        boolean foundValue = false;
        try {
            while (true) {
                String line = in.readLine();
                if (line == null) {
                       break;
                }
                if (line.contains(lookFor)) {
                    foundValue = true;
                    String replacedPrefix = getReplacedPrefix(lookFor);
                    if (replacedPrefix != null) {
                        int noccur = line.split(replacedPrefix, -1).length-1;
                        assertTrue("The option [" + lookFor + "] was not " +
                                   "replaced in the command line [" +
                                   line +"]", noccur == 1);
                    }
                    break;
                 }
            }
        } finally {
            in.close();
        }
        assertTrue("SN log does not indicate changed value. [" + lookFor +"]",
                   foundValue);
    }
}
