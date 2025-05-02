/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.IntParameter;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests some features of Parameter configuration.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeParameters extends StorageNodeTestBase {
    final static String fname = "testconfig.xml";
    final static int PORT = 5678;
    final static String PORTRANGE = "1,2";
    private static final int VERSION = 6;

    public StorageNodeParameters(boolean useThread) {
        super(useThread);
    }

    /**
     * Override superclass genParams method to provide use_thread as false
     * only for the tests in this class.
     */
    @Parameters(name="Use_Thread={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{false}});
    }

    /**
     * Notes: It is required to call the super methods if override
     * setUp and tearDown methods.
     */
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Add some parameters to a config file.
     */
    @Test
    public void testCreateConfig()
        throws Exception {

        StorageNodeId snid = new StorageNodeId(2);
        File configFile = new File(TestUtils.getTestDir(), fname);
        AdminId aid = new AdminId(1);
        AdminParams ap = createAdminParams(aid,
                                           snid,
                                           testhost);

        /**
         * StorageNodeParams.
         */
        StorageNodeParams sp =
            new StorageNodeParams(snid, testhost, PORT, "");
        sp.setRootDirPath(TestUtils.getTestDir().toString());
        sp.setHAHostname(testhost);
        sp.setHAPortRange(PORTRANGE);

        /**
         * GlobalParams.
         */
        GlobalParams gp = new GlobalParams(kvstoreName);

        /**
         * Add to configuration and save.  Change the version to test that.
         */
        LoadParameters lp = new LoadParameters();
        lp.addMap(ap.getMap());
        lp.addMap(sp.getMap());
        lp.addMap(gp.getMap());
        lp.setVersion(VERSION);
        lp.saveParameters(configFile);

        /**
         * Get a new object and make sure it's all good.
         */
        LoadParameters newLp = LoadParameters.getParameters(configFile, null);
        assertTrue(newLp.getVersion() == VERSION);
        assertTrue(null != newLp.getMap(ParameterState.GLOBAL_TYPE));
        assertTrue(null != newLp.getMap(aid.getFullName()));
        assertTrue(null != newLp.getMapByType(ParameterState.ADMIN_TYPE));
        assertTrue(null != newLp.getMap(ParameterState.SNA_TYPE));

        /**
         * Try to remove a badly-named param, make sure it fails.
         */
        assertTrue(newLp.removeMap("notpresent") == null);

        /**
         * Remove AdminParams.  This removes 4 components.
         */
        assertTrue(newLp.removeMap(aid.getFullName()) != null);
        newLp.saveParameters(configFile);
    }

    @Test
    public void testChangeParameters()
        throws Exception {

        final String newServicePortRange = "5000,6000";
        final String newLogFileLimit = "75";
        final String newMaxLink = "5000";
        final String newStartWait = "10000 s";

        StorageNodeAgentAPI snai =
            createRegisteredStore(new StorageNodeId(1), true);

        assertDefaults();

        /*
         * Assert that the values don't match the to-be-set values.
         */
        LoadParameters lp = snai.getParams();
        StorageNodeParams snp =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertFalse(snp.getLogFileLimit() ==
                    (Integer.parseInt(newLogFileLimit)));

        /*
         * Change a few parameters
         */
        ParameterMap map = new ParameterMap(ParameterState.SNA_TYPE,
                                            ParameterState.SNA_TYPE);
        map.setParameter(ParameterState.COMMON_SERVICE_PORTRANGE,
                         newServicePortRange);
        map.setParameter(ParameterState.SN_LOG_FILE_LIMIT, newLogFileLimit);
        map.setParameter(ParameterState.SN_MAX_LINK_COUNT, newMaxLink);
        map.setParameter(ParameterState.SN_REPNODE_START_WAIT, newStartWait);
        snai.newStorageNodeParameters(map);

        /*
         * Now try new params, they should match.
         */
        lp = snai.getParams();
        snp = new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertEquals(newServicePortRange, snp.getServicePortRange());
        assertEquals(Integer.parseInt(newLogFileLimit),
                     snp.getLogFileLimit());

        /*
         * Try a couple directly from the SNA.
         */
        assertEquals(Integer.parseInt(newMaxLink), sna.getMaxLink());
        assertEquals(10000, sna.getRepnodeWaitSecs());

        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    @Test
    public void testBadParameters() throws Exception {

        final String os = System.getProperty("os.name");
        String pathPrefix = File.separator;
        if (os.indexOf("Windows") >= 0) {
            pathPrefix = pathPrefix + File.separator;
        }
        final StorageNodeAgentAPI snai =
            createRegisteredStore(new StorageNodeId(1), true);
        assertDefaults();

        /**
         * Assert that the values don't match the to-be-set values.
         */
        LoadParameters lp = snai.getParams();
        StorageNodeParams snp =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertEquals(0L, snp.getRootDirSize());

        final String origRootPath = snp.getRootDirPath();

        final ParameterMap map = new ParameterMap(ParameterState.SNA_TYPE,
                                                  ParameterState.SNA_TYPE);
        map.setParameter(ParameterState.SN_ROOT_DIR_PATH, "DOESNOTEXIST");
        try {
            snai.newStorageNodeParameters(map);
            fail("Call should have thrown SNAFaultException");
        } catch (SNAFaultException sfe) {
            // expected
        }
        /* Check that nothing changed */
        lp = snai.getParams();
        snp = new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertEquals(origRootPath, snp.getRootDirPath());
        assertEquals(0L, snp.getRootDirSize());

        /* "/" should exist, while 1000 TB should be way too big */
        map.setParameter(ParameterState.SN_ROOT_DIR_PATH, "/");
        map.setParameter(ParameterState.SN_ROOT_DIR_SIZE, "10000 TB");
        try {
            snai.newStorageNodeParameters(map);
            fail("Call should have thrown SNAFaultException");
        } catch (SNAFaultException sfe) {
            // expected
        }
        lp = snai.getParams();
        snp = new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertEquals(origRootPath, snp.getRootDirPath());
        assertEquals(0L, snp.getRootDirSize());

        /* 100 should be OK */
        map.setParameter(ParameterState.SN_ROOT_DIR_SIZE, "100");
        snai.newStorageNodeParameters(map);

        /**
         * Verify the params were changed
         */
        lp = snai.getParams();
        snp = new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertEquals(100L, snp.getRootDirSize());

        /* Try with bogus storage dirs */
        final ParameterMap sdMap = BootstrapParams.createStorageDirMap();
        BootstrapParams.addStorageDir(sdMap,
                                      pathPrefix + "NOEXIST", "0");
        BootstrapParams.addStorageDir(sdMap,
                                      pathPrefix, "10000 TB");
        try {
            snai.newStorageNodeParameters(sdMap);
            fail("Call should have thrown SNAFaultException");
        } catch (SNAFaultException sfe) {
            // expected
        }

        /* Storage dir map will be null if not set */
        lp = snai.getParams();
        snp = new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        assertNull(snp.getStorageDirMap());

        snai.shutdown(true, true);
        assertShutdown(snai);
    }

    /**
     * Get the defaults and make sure they match.
     */
    private void assertDefaults() {
        DurationParameter execWait = (DurationParameter)
            DefaultParameter.getDefaultParameter
            (ParameterState.SN_LINK_EXEC_WAIT);
        IntParameter maxLink = (IntParameter)
            DefaultParameter.getDefaultParameter
            (ParameterState.SN_MAX_LINK_COUNT);
        DurationParameter repnodeWait = (DurationParameter)
            DefaultParameter.getDefaultParameter
            (ParameterState.SN_REPNODE_START_WAIT);
        DurationParameter serviceWait = (DurationParameter)
            DefaultParameter.getDefaultParameter
            (ParameterState.SN_SERVICE_STOP_WAIT);
        assertTrue(sna.getLinkExecWaitSecs() == execWait.toMillis()/1000);
        assertTrue(sna.getMaxLink() == maxLink.asInt());
        assertTrue(sna.getRepnodeWaitSecs() == repnodeWait.toMillis()/1000);
        assertTrue(sna.getServiceWaitMillis() == serviceWait.toMillis());
    }
}
