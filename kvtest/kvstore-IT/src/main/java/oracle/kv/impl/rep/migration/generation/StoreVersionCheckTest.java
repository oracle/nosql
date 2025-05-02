/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import oracle.kv.KVVersion;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

public class StoreVersionCheckTest extends TestBase {

    private static final StorageNodeId sn1 = new StorageNodeId(1);
    private static final KVVersion futureVer =
        new KVVersion(Integer.MAX_VALUE, Integer.MAX_VALUE,
                      Integer.MAX_VALUE, Integer.MAX_VALUE, 0, null);
    private static final KVVersion currVer = KVVersion.CURRENT_VERSION;
    private static final int lastMajor = currVer.getMajor() - 1;
    private static final KVVersion oldVer =
        new KVVersion(lastMajor, 0, 0, lastMajor + ".0.0");

    private CreateStore createStore;
    private static final int startPort = 5000;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
        }
        RepNodeService.startTestHook = null;
        RepNodeService.stopRequestedTestHook = null;

        LoggerUtils.closeAllHandlers();
        super.tearDown();
    }

    /**
     * Test that the API in RepNode should return expected results
     */
    @Test
    public void testGetStoreMinVerFromSNA() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* n SNs */
                                      1, /* rf */
                                      10, /* n partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true,
                                      null);
        createStore.start();
        disableVersionThread();
        final Topology topo = createStore.getAdmin().getTopology();
        /*
         * Since software version update runs at interval of 1 hours by
         * default, we mock the updater thread here and set the store version
         */
        setStoreVersion(KVVersion.CURRENT_VERSION);

        /* verify parameter for each rn */
        createStore.getRNs(sn1).forEach(rnId ->
                                            verifyGetStoreMinVer(rnId, topo));
    }

    /**
     * Test the new RepNode API from RN handle
     */
    @Test
    public void testCheckStoreVerAPI() throws Exception {

        /* Arrange to get the RepNodeService instance */
        final AtomicReference<RepNodeService> repNodeService =
            new AtomicReference<>();
        RepNodeService.startTestHook = repNodeService::set;

        /* Create the store using threads so the hooks will work */
        createStore = new CreateStore(
            kvstoreName, startPort, 1 /* SNs */, 1 /* RF */,
            10 /* partitions */, 1 /* capacity */,
            CreateStore.MB_PER_SN /* memoryMB */, true /* useThreads */,
            null /* mgmtImpl */);
        createStore.start();

        assertTrue("Waiting for RepNodeService",
                   new PollCondition(500, 10000) {
                       @Override
                       protected boolean condition() {
                           return repNodeService.get() != null;
                       }
                   }.await());

        final RepNodeService rns = repNodeService.get();
        if (rns == null) {
            fail("Cannot obtain RNS handle");
            return;
        }

        final RepNode rn = rns.getRepNode();
        if (rn == null) {
            fail("Cannot obtain RepNode handle");
            return;
        }
        
        disableVersionThread();

        /*
         * Since software version update runs at interval of 1 hours by
         * default, we mock the updater thread here and set the store version
         */
        setStoreVersion(oldVer);
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, currVer, false));
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, futureVer, false));
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, oldVer, true));

        setStoreVersion(currVer);
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, currVer, true));
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, futureVer, false));
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, oldVer, true));

        setStoreVersion(futureVer);
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, currVer, true));
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, futureVer, true));
        createStore.getRNs(sn1).forEach(rnId -> verifyRN(rn, oldVer, true));
    }

    private void verifyRN(RepNode rn, KVVersion verToCheck, boolean expected) {
        final boolean actual =
            rn.getStoreVersion().compareTo(verToCheck) >= 0;
        assertEquals(expected, actual);
    }

    private void verifyGetStoreMinVer(RepNodeId rid, Topology topo) {
        try {
            final RegistryUtils regUtl = new RegistryUtils(topo, null, logger);
            final KVVersion minVer = 
                getStoreMinimumVersion(sn1, regUtl);
            if (minVer == null) {
                fail("Cannot fetch min store version from SNA");
                return;
            }

            assertEquals(0, minVer.compareTo(KVVersion.CURRENT_VERSION));
            assertEquals(1, minVer.compareTo(KVVersion.PREREQUISITE_VERSION));
            assertEquals(-1, minVer.compareTo(futureVer));
        } catch (Exception exp) {
            fail("Cannot get min store version for " + rid);
        }
    }
    
    private void disableVersionThread() throws Exception {
        /* Disable the  KVAdminMetadata and KVVersion thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "false");
        final CommandServiceAPI admin = createStore.getAdmin();
        int p = admin.createChangeAllAdminsPlan("changeAdminParams", null, map);
        admin.approvePlan(p);
        admin.executePlan(p, false);
        admin.awaitPlan(p, 0, null);
        admin.assertSuccess(p);
    }

    private void setStoreVersion(KVVersion storeVersion) throws Exception {
        final ParameterMap pm =
            new ParameterMap(ParameterState.GLOBAL_TYPE,
                             ParameterState.GLOBAL_TYPE);
        pm.setParameter(ParameterState.GP_STORE_VERSION,
                        storeVersion.getNumericVersionString());
        final CommandServiceAPI admin = createStore.getAdmin();
        final int planId =
            admin.createChangeGlobalComponentsParamsPlan(
                "UpdateGlobalVersionMetadata", pm);
        try {
            admin.approvePlan(planId);
            admin.executePlan(planId, false);
            admin.awaitPlan(planId, 10 * 1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Encountered exception " +
                                      "running update store version plan.",
                       e);
            admin.cancelPlan(planId);
            throw e;
        }
    }
    
    private KVVersion getStoreMinimumVersion(StorageNodeId snId,
                                             RegistryUtils regUtils) {
    try {
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
        final ParameterMap gp =
        sna.getParams().getMapByType(ParameterState.GLOBAL_TYPE);
        final Parameter p = gp.get(ParameterState.GP_STORE_VERSION);
        if (p.asString() == null) {
            /* the parameter is not available */
            return null;
        }
        return KVVersion.parseVersion(p.asString());
        } catch (RemoteException | NotBoundException e) {
            logger.fine(() -> "Cannot ping SNA " + snId + " for parameters " +
            "reason: " + e.getMessage());
            /* give up and caller may try next time */
            return null;
        }
    }
}
