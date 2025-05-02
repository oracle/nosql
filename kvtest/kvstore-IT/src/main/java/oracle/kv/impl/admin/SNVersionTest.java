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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import oracle.kv.KVVersion;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.param.ParameterState.Type;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

public class SNVersionTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    @Override
    public void setUp()
        throws Exception {

        TestStatus.setActive(true);
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
     * Basic test of setting global store version on a newly
     * created store. Checks that the store version is updated
     * within two minutes.
     */
    @Test
    public void testBasic() throws Exception {

        final String first = "firstCandidate";
        final String dcName = "DataCenterA";
        final String poolName = Parameters.DEFAULT_POOL_NAME;

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      9 /* max of 9 SNs */,
                                      null /*snHosts */,
                                      dcName,
                                      3 /* repFactor*/,
                                      false /* useThreads*/,
                                      MB_PER_SN_STRING /* snaMemoryMB */,
                                      DatacenterType.PRIMARY /*dcType */,
                                      true /* allowArbiters */,
                                      0 /*rnCachePercent */,
                                      false /* masterAffinity */,
                                      false /* disableAdminThreads */);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();

        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), null,
                        1, 2, 3, 4, 5, 6, 7, 8);

        /* make an initial topology */
        cs.createTopology(first, poolName, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, true);

        Parameters parms = cs.getParameters();
        GlobalParams gp = parms.getGlobalParams();
        ParameterMap pm = gp.getGlobalComponentsPolicies();
        /*
         * Check to insure that the kvstore version has been set.
         */
        assertEquals(KVVersion.CURRENT_VERSION.getNumericVersionString(),
                     pm.get(ParameterState.GP_STORE_VERSION).asString());
    }

    /**
     * Tests that sn and global software version is updated 
     * on "older" nodes. Simulates new software being brought up on
     * older version of metadata with respect to the software version.
     */
    @Test
    public void testUpgrade() throws Exception {

        final String first = "firstCandidate";
        final String dcName = "DataCenterA";
        final String poolName = Parameters.DEFAULT_POOL_NAME;

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      9 /* max of 9 SNs */,
                                      null /*snHosts */,
                                      dcName,
                                      3 /* repFactor*/,
                                      false /* useThreads*/,
                                      MB_PER_SN_STRING /* snaMemoryMB */,
                                      DatacenterType.PRIMARY /*dcType */,
                                      true /* allowArbiters */,
                                      0 /*rnCachePercent */,
                                      false /* masterAffinity */,
                                      false /* disableAdminThreads */);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();

        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), null,
                        1, 2, 3, 4, 5, 6, 7, 8);
        final int lastMajor = KVVersion.CURRENT_VERSION.getMajor() - 1;
        final String lastVersion = lastMajor + ".0.0";
        snSet.changeParams(cs, ParameterState.SN_SOFTWARE_VERSION,
                           lastVersion,
                           1,2,3,4,5,6,7,8);
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter(ParameterState.GP_STORE_VERSION, lastVersion);

        int p =
            cs.createChangeGlobalComponentsParamsPlan(
                "Test change of GP_STORE_VERSION", map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        
        shutdownSNs(0);
        snSet.restart(0, Collections.emptySet(), logger);
        cs = StorageNodeUtils.waitForAdmin("localhost",
                                           snSet.getRegistryPort(0), 5);

        /* make an initial topology */
        cs.createTopology(first, poolName, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, true);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);
        waitForStoreVersion(cs);

        Parameters parms = cs.getParameters();
        GlobalParams gp = parms.getGlobalParams();
        ParameterMap pm = gp.getGlobalComponentsPolicies();

        /*
         * Check to insure that the kvstore version has been set.
         */
        assertEquals(KVVersion.CURRENT_VERSION.getNumericVersionString(),
                     pm.get(ParameterState.GP_STORE_VERSION).asString());
    }
    
    /**
     * Tests that older SN cannot be added to a KVStore.
     */
    @Test
    public void testAddOldSN() throws Exception {

        final String dcName = "DataCenterA";

        Map<String, ParameterState> pMap = ParameterState.getMap();
        ParameterState oldps = pMap.get(ParameterState.SN_SOFTWARE_VERSION);

        Object dp = new DefaultParameter().
                        create(ParameterState.SN_SOFTWARE_VERSION,
                               ParameterState.SN_SOFTWARE_VERSION_DEFAULT, 
                               Type.STRING);
        ParameterState newps =
            new ParameterState(oldps.getType(),
                               dp,
                               EnumSet.of(Info.BOOT, Info.HIDDEN,
                                          Info.NORESTART),
                               oldps.getScope(), 0, 0, null);
       pMap.put(ParameterState.SN_SOFTWARE_VERSION, newps);

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      9 /* max of 9 SNs */,
                                      null /*snHosts */,
                                      dcName,
                                      3 /* repFactor*/,
                                      false /* useThreads*/,
                                      MB_PER_SN_STRING /* snaMemoryMB */,
                                      DatacenterType.PRIMARY /*dcType */,
                                      true /* allowArbiters */,
                                      0 /*rnCachePercent */,
                                      false /* masterAffinity */,
                                      false /* disableAdminThreads */);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();

        SNSet snSet = sysAdminInfo.getSNSet();
        
        KVVersion newerVersion = 
            new KVVersion(KVVersion.CURRENT_VERSION.getMajor(), 
                          KVVersion.CURRENT_VERSION.getMinor() + 1, 0, null);
        snSet.changeParams(cs, ParameterState.SN_SOFTWARE_VERSION,
                           newerVersion.getNumericVersionString(), 
                           0);
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter(ParameterState.GP_STORE_VERSION,
                         newerVersion.getNumericVersionString());
                         
        int p =
            cs.createChangeGlobalComponentsParamsPlan(
                "Test change of GP_STORE_VERSION", map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        
        Exception gotEx = null;
        try {
            snSet.deploy(cs, sysAdminInfo.getDCId(dcName), null,
                            1);
        } catch (Exception e) {
            gotEx = e;
        }
        assertTrue("SN was deployed, expected an exception.",gotEx != null);
        
    } 

    private void waitForStoreVersion(CommandServiceAPI cs)
            throws RemoteException {
        final long MAX_WAIT = 1000 * 2 * 60;

        boolean done = false;
        long endTime = System.currentTimeMillis() + MAX_WAIT;

        while (!done &&
               System.currentTimeMillis() < endTime  ) {
            Parameters parms = cs.getParameters();
            GlobalParams gp = parms.getGlobalParams();
            ParameterMap pm = gp.getGlobalComponentsPolicies();
            String storeVersion =
                pm.get(ParameterState.GP_STORE_VERSION).asString();
            if (storeVersion.equals(
                   KVVersion.CURRENT_VERSION.getNumericVersionString())) {
                done = true;
            }
        }
    }


    private void runDeploy(String planName,
                           CommandServiceAPI cs,
                           String candidateName,
                           boolean isSuccess)
        throws RemoteException {

        try {
            DeployUtils.printCandidate(cs, candidateName, logger);
            int planNum = cs.createDeployTopologyPlan(planName,
                                                      candidateName,
                                                      null);
            cs.approvePlan(planNum);
            cs.executePlan(planNum, false);
            cs.awaitPlan(planNum, 0, null);
            if (isSuccess) {
                cs.assertSuccess(planNum);
            }
            DeployUtils.printCurrentTopo(cs, "Deploy Topology plan "+
                                         planName, logger);
        } catch (RuntimeException e) {
            logger.severe(LoggerUtils.getStackTrace(e));
            logger.severe(cs.validateTopology(null));
            logger.severe(cs.validateTopology(candidateName));
            throw e;
        }
    }

    private void shutdownSNs(final int... sns) {
        SNSet snSet = sysAdminInfo.getSNSet();
        for (int sn : sns) {
            snSet.shutdown(sn, logger);
        }

        final boolean ok = new PollCondition(1000, 30000) {
            @Override
            protected boolean condition() {
                for (int sn : sns) {
                    try {
                        RegistryUtils.getAdmin(
                            "localhost", snSet.getRegistryPort(sn),
                            null /* loginMgr */, logger).ping();
                        logger.info("Admin " + sn + " is still active");
                        return false;
                    } catch (RemoteException | NotBoundException e) {
                        continue;
                    } catch (Exception e) {
                        logger.info("Problem with Admin " + sn + ": " + e);
                        return false;
                    }
                }
                return true;
            }
        }.await();
        assertTrue("Wait for admins to stop", ok);
    }
}
