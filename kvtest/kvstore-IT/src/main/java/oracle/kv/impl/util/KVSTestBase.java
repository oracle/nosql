/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import java.rmi.RemoteException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * The test base class for tests whose starting point is a configured KVS. The
 * initTopology method can be used to configure and deploy the KVS before
 * embarking on the actual tests.
 */
public class KVSTestBase extends TestBase {

    protected static final LoginManager NULL_LOGIN_MGR = null;
    protected SysAdminInfo sysAdminInfo;
    protected Topology topo;
    protected RegistryUtils regUtils;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.clearTestDirectory();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();

        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
        }

        LoggerUtils.closeAllHandlers();
    }

    protected void changeZoneAffinityAndDeploy(String dcName,
                                               boolean newAffinity)
            throws RemoteException {
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        cs.copyTopology("TopoA", "newTopo");
        cs.changeZoneMasterAffinity("newTopo", sysAdminInfo.getDCId(dcName),
                                    newAffinity);
        final int planNum =
                cs.createDeployTopologyPlan("RedeployedTopo", "newTopo", null);

        executePlan(planNum);

        topo = cs.getTopology();
        regUtils = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
    }

    protected void initMultipleDCsTopo(int numSNs,
                                       int[] capacities,
                                       int numDCs,
                                       int[] RFs,
                                       int[] assignedSNsToDCs,
                                       boolean[] affinities) throws Exception {
        boolean[] isPrimaries = new boolean[numDCs];
        for (int i=0; i<numDCs; i++) {
            isPrimaries[i] = true;
        }
        initMultipleDCsTopo(numSNs, capacities, numDCs, isPrimaries, RFs,
                            assignedSNsToDCs, affinities);
    }

    protected void initMultipleDCsTopo(int numSNs,
                                       int[] capacities,
                                       int numDCs,
                                       boolean[] isPrimaries,
                                       int[] RFs,
                                       int[] assignedSNsToDCs,
                                       boolean[] affinities) throws Exception {

        if (capacities.length != numSNs) {
            throw new Exception("The length of capacites should be equal " +
                                "the number of SNs");
        }

        if (isPrimaries.length != numDCs) {
            throw new Exception("The length of isPrimaries should be equal " +
                                "the number of DCs");
        }

        if (RFs.length != numDCs) {
            throw new Exception("The length of RFs should be equal " +
                                "the number of DCs");
        }

        if (affinities.length != numDCs) {
            throw new Exception("The length of affinities should be " +
                                "equal the number of DCs");
        }

        if (assignedSNsToDCs.length != numDCs) {
            throw new Exception("The length of assignedSNsToDCs should be " +
                                "equal the number of DCs");
        }

        if (numSNs <= 0) {
            throw new Exception("The number of SNs should be greater than 0");
        }

        if (numDCs <= 0) {
            throw new Exception("The number of DCs should be greater than 0");
        }

        if (!isPrimaries[0]) {
            throw new Exception("The first DC should be primary DC");
        }

        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName, numSNs, "DC1",
                                                 RFs[0], affinities[0]);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet sns = sysAdminInfo.getSNSet();

        /* Index for SN, after deploy a SN, snIndex is incremented */
        int snIndex = 0;

        /* Deploy SNs for the first DC */
        for (int i=0; i<assignedSNsToDCs[0]; i++) {
            sns.deploy(cs, sysAdminInfo.getDCId("DC1"), MB_PER_SN_STRING,
                       snIndex);
            sns.changeParams(cs, ParameterState.COMMON_CAPACITY,
                             String.valueOf(capacities[snIndex]), snIndex);
            snIndex++;
        }

        /* Deploy other DCs and SNs */
        DatacenterType dcType;
        for (int i=1; i<numDCs; i++) {
            String DCName = "DC" + (i+1);
            if (isPrimaries[i]) {
                dcType = DatacenterType.PRIMARY;
            } else {
                dcType = DatacenterType.SECONDARY;
            }
            AdminUtils.deployDatacenter(cs, DCName, RFs[i], dcType,
                                        sysAdminInfo, false /*allowArbiters*/,
                                        affinities[i]);

            for (int j=0; j<assignedSNsToDCs[i]; j++) {
                sns.deploy(cs, sysAdminInfo.getDCId(DCName), MB_PER_SN_STRING,
                           snIndex);
                sns.changeParams(cs, ParameterState.COMMON_CAPACITY,
                                 String.valueOf(capacities[snIndex]), snIndex);

                snIndex++;
            }
        }


        /* Deploy topology */
        cs.createTopology("TopoA", Parameters.DEFAULT_POOL_NAME, 100, false);
        final int planNum =
            cs.createDeployTopologyPlan("DeployTopology", "TopoA", null);
        executePlan(planNum);

        topo = cs.getTopology();
        // TopologyPrinter.printTopology(topo, System.err);
        regUtils = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
    }

    protected void initTopology(int RF,
                                int memMB,
                                int numSNs,
                                int ... snCapacity)
        throws Exception {

        final String poolName = "pool";
        final String candidateName = "candidate";
        final String planName = "plan1";
        final String dcName = "DataCenterA";

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 numSNs,
                                                 dcName,
                                                 RF /* repFactor*/);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        int whichSNs[] = new int[numSNs];
        for (int i=0; i < numSNs; i++) {
            whichSNs[i] = i;
        }
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName),
                     Integer.toString(memMB), whichSNs);
        int totalCapacity = 0;
        for (int i=0; i < numSNs; i++) {
            snSet.changeParams(cs, ParameterState.COMMON_CAPACITY,
                               Integer.toString(snCapacity[i]), whichSNs[i]);
            totalCapacity += snCapacity[i];
        }

        AdminUtils.makeSNPool(cs, poolName, snSet.getIds(whichSNs));
        cs.createTopology(candidateName,
                          poolName,
                          (totalCapacity / RF) * 10,
                          false);

        int planNum = cs.createDeployTopologyPlan(planName, candidateName,
                                                  null);
        executePlan(planNum);
        topo = cs.getTopology();
        // TopologyPrinter.printTopology(topo, System.err);
        regUtils = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
    }

    protected void executePlan(int planNum)
        throws RemoteException {
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);

        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
    }
}
