/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.param;

import static org.junit.Assert.assertEquals;

import oracle.kv.KVVersion;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Assert;
import org.junit.Test;

public class ChangeGlobalParamsTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Test to insure that global parameters may be changed
     * when an SN is down. Also exercises the code path
     * of concurrent access to the SN's configuration file. The version
     * update thread, parameter consistency thread and the
     * test thread cause access to the SN configuration file.
     */
    @Test
    public void testGlobalParamsSNDown()
        throws Exception {
        testGlobalParamsInternal(true);
    }

    /**
     * Exercises the code path
     * of concurrent access to the SN's configuration file. The version
     * update thread, parameter consistency thread and the
     * test thread cause access to the SN configuration file.
     */
    @Test
    public void testGlobalParams()
        throws Exception {
        testGlobalParamsInternal(false);
    }

    private void testGlobalParamsInternal(boolean killSN)
                    throws Exception {
        final long MAX_WAIT_MS = 2 * 60 * 1000;
        int snToKill = 3;

        createStore = new CreateStore(kvstoreName,
                        startPort,
                        9, /* Storage Nodes */
                        3, /* Replication Factor */
                        300, /* Partitions */
                        1 /* capacity */);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();

        /* Change the interval for SN consistency checking and correction */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_INTERVAL, "10 SECONDS");
        map.setParameter(ParameterState.AP_VERSION_CHECK_INTERVAL,
                         "10 SECONDS");

        /*
         * Enable admin threads that may create and execute plans.
         */
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "true") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "true");
        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);

        if (killSN) {
            createStore.shutdownSNA(snToKill, true);
        }
        cs = createStore.getAdmin();
        String newDuration = "10 SECONDS";
        map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter("collectorInterval", newDuration);
        p =
            cs.createChangeGlobalComponentsParamsPlan("change stats interval",
                                                      map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        if (!killSN) {
            cs.assertSuccess(p);
        }

        StorageNodeAgent sna =  createStore.getStorageNodeAgent(1);
        long ci = sna.getCollectorInterval();
        assertEquals(ci, 10000);
        if (killSN) {
            createStore.startSNA(snToKill);
            sna =  createStore.getStorageNodeAgent(snToKill);
            long startTime = System.currentTimeMillis();
            while ((System.currentTimeMillis() - startTime) < MAX_WAIT_MS) {
                ci = sna.getCollectorInterval();
                if (ci == 20000) {
                    Thread.sleep(1000);
                } else {
                    break;
                }
            }
            assertEquals(10000, ci);
        }

        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() < (startTime + 60000)) {
            Parameters dbParams = cs.getParameters();
            int updatedVersionCount = 0;
            for (StorageNodeParams snp : dbParams.getStorageNodeParams()) {
                Parameter versionP =
                    snp.getMap().get(ParameterState.SN_SOFTWARE_VERSION);
                if (versionP.asString() == null ||
                    !versionP.asString().equals(
                        KVVersion.CURRENT_VERSION.getNumericVersionString())) {
                    Thread.sleep(5000);
                    break;
                }
                updatedVersionCount++;
            }
            if (updatedVersionCount == dbParams.getStorageNodeParams().size()) {
                return;
            }
        }

        Parameters dbParams = cs.getParameters();
        for (StorageNodeParams snp : dbParams.getStorageNodeParams()) {
            Parameter versionP =
                snp.getMap().get(ParameterState.SN_SOFTWARE_VERSION);
            Assert.assertTrue(versionP.asString() != null);
            Assert.assertTrue(
                versionP.asString().equals(
                    KVVersion.CURRENT_VERSION.getNumericVersionString()));
        }
    }
}
