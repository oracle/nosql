/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static oracle.kv.KVVersion.CURRENT_VERSION;
import static oracle.kv.KVVersion.PREREQUISITE_VERSION;
import static oracle.kv.util.TestUtils.checkException;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the upgrade and version checking operations of storage node.
 */
@RunWith(FilterableParameterized.class)
public class StorageNodeUpgrade extends StorageNodeTestBase {

    public StorageNodeUpgrade(boolean useThread) {
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
     * Tests null version. This is the condition when upgrading from a
     * pre-versioned release which is not supported.
     */
    @Test
    public void testNullVersion() throws Exception {
        checkException(() -> startSNA(null),
                       IllegalStateException.class,
                       "version missing");
    }

    /**
     * Tests the previous version not meeting the prerequisite.
     */
    @Test
    public void testBadPrerequisite() throws Exception {
        checkException(() -> startSNA(new KVVersion(0, 0, 0, 0, 0, null)),
                       IllegalStateException.class,
                       "not satisfy the prerequisite");

    }

    /**
     * Tests the previous version just meeting the prerequisite.
     */
    @Test
    public void testPrerequisite() throws Exception {
        startSNA(PREREQUISITE_VERSION);
    }

    /**
     * Test the previous version being later then the current version.
     */
    @Test
    public void testNewerVersion() throws Exception {
        checkException(() ->
                        startSNA(new KVVersion(CURRENT_VERSION.getOracleMajor(),
                                               CURRENT_VERSION.getOracleMinor(),
                                               CURRENT_VERSION.getMajor(),
                                               CURRENT_VERSION.getMinor() + 1,
                                               CURRENT_VERSION.getPatch(),
                                               null)),
                       IllegalStateException.class,
                       "cannot be downgraded");
    }

    /**
     * Test downgrading within the same minor version
     */
    @Test
    public void testGoodDowngrade() throws Exception {
        startSNA(new KVVersion(CURRENT_VERSION.getOracleMajor(),
                               CURRENT_VERSION.getOracleMinor(),
                               CURRENT_VERSION.getMajor(),
                               CURRENT_VERSION.getMinor(),
                               CURRENT_VERSION.getPatch() + 10,
                               null));
    }

    /**
     * Test downgrading from a later minor version
     */
    @Test
    public void testBadDowngrade() throws Exception {
        checkException(() ->
                        startSNA(new KVVersion(CURRENT_VERSION.getOracleMajor(),
                                               CURRENT_VERSION.getOracleMinor(),
                                               CURRENT_VERSION.getMajor(),
                                               CURRENT_VERSION.getMinor() + 1,
                                               0,
                                               null)),
                       IllegalStateException.class,
                       "cannot be downgraded");
    }

    /**
     * Test the condition where the store version is a different patch version
     * than the current software version.
     */
    @Test
    public void testStoreVersionPatchDowngrade() throws Exception {
        startSNA(CURRENT_VERSION,
                 new KVVersion(CURRENT_VERSION.getOracleMajor(),
                               CURRENT_VERSION.getOracleMinor(),
                               CURRENT_VERSION.getMajor(),
                               CURRENT_VERSION.getMinor(),
                               CURRENT_VERSION.getPatch() + 10,
                               null));
    }

    /**
     * Test the condition where the store version is newer than the current
     * software version.
     */
    @Test
    public void testStoreVersionDowgrade() throws Exception {
        checkException(() ->
                        startSNA(CURRENT_VERSION,
                                 new KVVersion(CURRENT_VERSION.getOracleMajor(),
                                               CURRENT_VERSION.getOracleMinor(),
                                               CURRENT_VERSION.getMajor(),
                                               CURRENT_VERSION.getMinor() + 1,
                                               0,
                                               null)),
                       IllegalStateException.class,
                       "is not supported");
    }

    /**
     * Test the condition were the current software is compatible with the
     * previous version, but not the store version.
     */
    @Test
    public void testStorePrerequisite() throws Exception {
        assert PREREQUISITE_VERSION.getMajor() > 0;

        /*
         * The previous version was at PREREQUISITE_VERSION, but the store
         * version has not been updated and is older than the prereq.
         */
        checkException(() ->
                startSNA(PREREQUISITE_VERSION,
                         new KVVersion(PREREQUISITE_VERSION.getOracleMajor(),
                                       PREREQUISITE_VERSION.getOracleMinor(),
                                       PREREQUISITE_VERSION.getMajor() - 1,
                                       PREREQUISITE_VERSION.getMinor(),
                                       0,
                                       null)),
                       IllegalStateException.class,
                       "not satisfy the prerequisite");
    }

    /**
     * Starts the SNA after setting the version in the boot parameters to the
     * specified version. If previousVersion is null, no version entry will be
     * in the boot parameters.
     *
     * @param previousVersion version to set in the boot parameters before the
     *                        sna starts or null
     * @throws Exception
     */
    private void startSNA(KVVersion previousVersion) throws Exception {
        startSNA(previousVersion, null);
    }

    /**
     * Starts the SNA after setting the software version and the store version
     * in the boot parameters to the specified versions. If previousVersion is
     * null, no version entry will be in the boot parameters.
     *
     * @param previousVersion software version to set or null
     * @param storeVersion store version to set or null
     */
    private void startSNA(KVVersion previousVersion,
                          KVVersion storeVersion) throws Exception {
        final String testDir = TestUtils.getTestDir().toString();
        final String configFile = testDir + File.separator + CONFIG_FILE_NAME;
        final String portRange = "1,2";

        TestUtils.generateBootstrapFile(configFile, testDir, testhost,
                                        portFinder.getRegistryPort(),
                                        portRange, testhost, false, 1, null);

        BootstrapParams bp =
                    ConfigUtils.getBootstrapParams(new File(configFile));

        bp.getMap().setParameter(ParameterState.SN_SOFTWARE_VERSION,
                                 (previousVersion == null) ?
                                     null :
                                     previousVersion.getNumericVersionString());

        if (storeVersion != null) {
            bp.setStoreVersion(storeVersion.getNumericVersionString());
        }
        ConfigUtils.createBootstrapConfig(bp, configFile);

        sna = startSNA(testDir, CONFIG_FILE_NAME, false, true);
        int port = portFinder.getRegistryPort();
        StorageNodeAgentAPI snai =
            RegistryUtils.getStorageNodeAgent(testhost,
                                              port,
                                              sna.getServiceName(),
                                              getLoginMgr(),
                                              logger);
        snai.shutdown(true, true);
    }
}
