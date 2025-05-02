/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.param;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.VerifyConfiguration.AvailableStorageLow;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.kvlite.KVLite;

import com.sleepycat.je.rep.NodeType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test no-restart parameter changes to Admin and RepNode.
 */
@RunWith(FilterableParameterized.class)
public class ChangeParametersTest extends TestBase {

    private static final String testdir = TestUtils.getTestDir().toString();

    private static final String storeName = "cpstore";
    private static final String testhost = "localhost";
    private static final int startPort = 6000;
    private static final int haRange = 2;
    private static final String pollInterval = "2 SECONDS";
    private static final String threadDumpInterval = "3 SECONDS";

    private PortFinder portFinder;
    private KVLite kvlite;

    private boolean embedded;

    /**
     * Set the embedded value for this run of the test suite.
     */
    public ChangeParametersTest(boolean isEmbedded) {
        embedded = isEmbedded;
    }

    /**
     * @return the input data for parameterized tests.
     */
    @Parameterized.Parameters(name="embedded={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();
        if (embedded) {
            EmbeddedMode.setEmbedded(true);
        }
        portFinder = new PortFinder(startPort, haRange);
        TestStatus.setManyRNs(true);

        /* use threads to avoid process cleanup */
        kvlite = new KVLite(testdir,
                            storeName,
                            portFinder.getRegistryPort(),
                            true,
                            testhost,
                            portFinder.getHaRange(),
                            null,
                            0,
                            null,
                            false,
                            false,
                            null,
                            -1);
        kvlite.setPolicyMap(mergeParameterMapDefaults(createPolicyMap()));
        kvlite.setVerbose(false);
        kvlite.start();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (kvlite != null) {
            kvlite.stop(false);
            kvlite = null;
        }
        LoggerUtils.closeAllHandlers();
        if (embedded) {
            EmbeddedMode.setEmbedded(false);
        }
    }

    /**
     * Exercise the basic interfaces, setting defaults, globals, and
     * per-node parameters.
     */
    @Test
    public void testBasic()
        throws Exception {

        CommandServiceAPI admin = StorageNodeTestBase.waitForAdmin
            (testhost, portFinder.getRegistryPort());
        assertNotNull(admin);
        StorageNodeAgentAPI snai = kvlite.getSNAPI();
        assertNotNull(snai);
        Parameters params = admin.getParameters();
        /* "know" that AdminId is 1 and RepNode is 1,1 */
        AdminId adId = new AdminId(1);
        RepNodeId rnId = new RepNodeId(1, 1);
        RepNodeParams rnp = params.get(rnId);
        assertNotNull(rnp);
        AdminParams ap = params.get(adId);
        assertNotNull(ap);
        assertFalse(ap.createCSV());

        /* assert that the values of the policy changes are correct */
        ParameterMap adminMap = ap.getMap().copy();
        ParameterMap rnMap = rnp.getMap();
        DurationParameter dp =
            (DurationParameter) adminMap.get(ParameterState.MP_POLL_PERIOD);
        assertNotNull(dp);
        assertTrue(dp.asString().equals(pollInterval));
        DurationParameter interval =
            (DurationParameter) rnMap.get(ParameterState.SP_DUMP_INTERVAL);
        assertTrue(interval.asString().equals(threadDumpInterval));

        /* Modify the admin */
        dp.setMillis(1000);
        /* Modify event expiry age but don't check that until later */
        dp = (DurationParameter)
            adminMap.get(ParameterState.AP_EVENT_EXPIRY_AGE);
        assertNotNull(dp);
        long originalExpiry = dp.toMillis();
        dp.setMillis(originalExpiry * 2);
        adminMap.setParameter(ParameterState.MP_CREATE_CSV, "true");

        /* Set JVM_LOGGING which should take effect on restart */
        adminMap.setParameter
            (ParameterState.JVM_LOGGING,
             "oracle.kv.level=INFO;oracle.kv.util.ConsoleHandler.level=ALL;" +
             "oracle.kv.util.FileHandler.level=INFO");
        snai.newAdminParameters(adminMap);

        /* now, notify admin */
        admin.newParameters();

        /* verify new parameters */
        LoadParameters lp = admin.getParams();
        ParameterMap map = lp.getMapByType(ParameterState.ADMIN_TYPE);
        assertTrue(map != null);
        assertTrue(adminMap.equals(map));
        AdminParams newAp = new AdminParams(map);
        assertTrue(newAp.createCSV());

        /* cause the admin to restart and verify again */
        admin.stop(false);
        admin = StorageNodeTestBase.waitForAdmin
            (testhost, portFinder.getRegistryPort());
        lp = admin.getParams();
        map = lp.getMapByType(ParameterState.ADMIN_TYPE);
        assertTrue(map != null);
        assertTrue(adminMap.equals(map));
        newAp = new AdminParams(map);
        assertTrue(newAp.createCSV());
        assertTrue(newAp.getEventExpiryAge() == 2 * originalExpiry);

        /* Modify the RepNode */
        interval.setMillis(500);
        rnMap.setParameter(ParameterState.SP_COLLECT_ENV_STATS, "true");
        snai.newRepNodeParameters(rnMap);
        RepNodeAdminAPI rna = StorageNodeTestBase.waitForRNAdmin
            (rnId, kvlite.getStorageNodeId(), storeName, testhost,
             portFinder.getRegistryPort(), 5);
        rna.newParameters();

        /*
         * Verify new parameters.
         */
        lp = rna.getParams();
        map = lp.getMapByType(ParameterState.REPNODE_TYPE);
        assertTrue(map != null);
        assertTrue(rnMap.equals(map));
    }

    /**
     * This test creates a mismatch of parameters between the Admin and a
     * Storage Node and makes sure that VerifyConfiguration finds it.
     */
    @Test
    public void testBadParameters()
        throws Exception {

        CommandServiceAPI admin = StorageNodeTestBase.waitForAdmin
            (testhost, portFinder.getRegistryPort());
        assertNotNull(admin);
        StorageNodeAgentAPI snai = kvlite.getSNAPI();
        assertNotNull(snai);
        Parameters params = admin.getParameters();

        /*
         * Configuration should be fine. Since storage space is 1 GB, so
         * we are expected to get Low Disk Space warnings. We will explicitly
         * remove them for test purpose.
         */
        VerifyResults results = admin.verifyConfiguration(false, true, false);
        Iterator<Problem> warnItr = results.getWarnings().iterator();
        while (warnItr.hasNext()) {
            final Problem probWarning = warnItr.next();
            if ((probWarning instanceof MissingRootDirectorySize) ||
                (probWarning instanceof AvailableStorageLow)) {
                warnItr.remove();
            }
        }

        assertTrue(results.display(), results.okay());

        /*
         * Now, add an extra RN directly to the SNA to provoke a verification
         * failure.  First create the parameters for the new node by adding one
         * to the HA port for the original RN.
         */
        RepNodeId origRn = new RepNodeId(1, 1);
        RepNodeId newRn = new RepNodeId(2, 2);
        RepNodeParams rnp = params.get(origRn);
        StorageNodeParams snp = params.get(rnp.getStorageNodeId());
        int haPort = rnp.getHAPort() + 1;
        RepNodeParams newRnp = new RepNodeParams(snp.getStorageNodeId(),
                                                 newRn,
                                                 false,
                                                 snp.getHAHostname(),
                                                 haPort,
                                                 snp.getHAHostname(),
                                                 haPort,
                                                 null, /* mountPoint */
                                                 NodeType.ELECTABLE);
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                new HashSet<Metadata<? extends MetadataInfo>>(1);
        metadataSet.add(admin.getTopology());
        assertTrue(snai.createRepNode(newRnp.getMap(), metadataSet));
        results = admin.verifyConfiguration(false, true, false);

        /*
         * Since storage space is 1 GB, so we are expected to get
         * Low Disk Space warnings. We will explicitly remove them
         * for test purpose.
         */
        warnItr = results.getWarnings().iterator();
        while (warnItr.hasNext()) {
            if (warnItr.next() instanceof AvailableStorageLow) {
                warnItr.remove();
            }
        }

        /*
         * There should be one problem.
         */
        assertTrue(results.display(), results.numViolations() == 1);
    }

    /**
     * Create some default policy parameters to help test the change mechanism.
     * NOTE: parameters are merged vs replaced so this can be a partial map.
     */
    private ParameterMap createPolicyMap() {
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.MP_POLL_PERIOD, pollInterval);
        map.setParameter(ParameterState.SP_DUMP_INTERVAL, threadDumpInterval);
        map.setParameter(ParameterState.SN_SERVICE_STOP_WAIT, "10000 ms");
        return map;
    }
}
