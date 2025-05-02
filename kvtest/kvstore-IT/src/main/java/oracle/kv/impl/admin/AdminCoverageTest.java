/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.ssl.SSLTransport;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;

import org.junit.Test;

/**
 * Unit tests that provide test coverage for the
 * <code>oracle.kv.impl.admin.Admin</code> class.
 */
public class AdminCoverageTest
                 extends AdminTestBase {

    private static String THIS_CLASS_NAME =
                              AdminCoverageTest.class.getSimpleName();

    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
    private static final String F_SEP = System.getProperty("file.separator");
    private static final int MAX_PORT_VAL = 65535;
    private static final int CUR_MAJOR = KVVersion.CURRENT_VERSION.getMajor();
    private static final int CUR_MINOR = KVVersion.CURRENT_VERSION.getMinor();
    private static final int CUR_PATCH = KVVersion.CURRENT_VERSION.getPatch();
    private static final KVVersion NEXT_KV_MINOR_VERSION =
        new KVVersion(CUR_MAJOR, (CUR_MINOR + 1), CUR_PATCH, null);
    private static final KVVersion PREV_KV_MINOR_VERSION =
        new KVVersion(CUR_MAJOR, (CUR_MINOR - 1), CUR_PATCH, null);
    /* Version guaranteed to not meet current prerequisite. */
    private static final KVVersion OLD_KV_MAJOR_VERSION =
        new KVVersion(17, CUR_MINOR, CUR_PATCH, null);

    private static final int REGISTRY_PORT = MAX_PORT_VAL - 5000;
    private static final String HA_PORT_RANGE =
        REGISTRY_PORT + 1 + "," + REGISTRY_PORT + 2 + "," +
        REGISTRY_PORT + 3 + "," + REGISTRY_PORT + 4 + "," +
        REGISTRY_PORT + 5 + "," + REGISTRY_PORT + 6 + "," +
        REGISTRY_PORT + 7;
    private static final String SERVICE_PORT_RANGE =
        REGISTRY_PORT + 8 + "," + REGISTRY_PORT + 9 + "," +
        REGISTRY_PORT + 10 + "," + REGISTRY_PORT + 11 + "," +
        REGISTRY_PORT + 12 + "," + REGISTRY_PORT + 13 + "," +
        REGISTRY_PORT + 14;
    private static final int ADMIN_WEB_SERVICE_PORT = REGISTRY_PORT - 1;

    private BootstrapParams mockedBootstrapParams =
                                createMock(BootstrapParams.class);
    private SecurityParams mockedSecurityParams =
                               createMock(SecurityParams.class);
    private GlobalParams mockedGlobalParams = createMock(GlobalParams.class);
    private StorageNodeParams mockedStorageNodeParams =
                                  createMock(StorageNodeParams.class);
    private AdminParams mockedAdmin1Params = createMock(AdminParams.class);
    private AdminParams mockedAdmin2Params = createMock(AdminParams.class);
    private LoadParameters mockedLoadParameters =
                               createMock(LoadParameters.class);
    private Parameters mockedParameters = createMock(Parameters.class);
    private RepNodeParams mockedRepNodeParams = createMock(RepNodeParams.class);
    private ArbNodeParams mockedArbNodeParams = createMock(ArbNodeParams.class);
    private AdminStores mockedAdminStores = createMock(AdminStores.class);
    private AdminStores mockedAdminStoresNullParams =
                            createMock(AdminStores.class);
    private Admin.StartupStatus mockedStartupStatus =
                                    createMock(Admin.StartupStatus.class);

    private ReplicatedEnvironment mockedReplicatedEnvironment =
                                      createMock(ReplicatedEnvironment.class);
    private StateChangeEvent mockedStateChangeEventMaster =
                                 createMock(StateChangeEvent.class);
    private StateChangeEvent mockedStateChangeEventReplica =
                                 createMock(StateChangeEvent.class);
    private Transaction mockedTransaction = createMock(Transaction.class);

    private Topology mockedTopology = createMock(Topology.class);
    private TopologyStore mockedTopologyStore =
                              createMock(TopologyStore.class);
    private TopologyCandidate mockedTopologyCandidate =
                                  createMock(TopologyCandidate.class);
    private Datacenter mockedDatacenter1 = createMock(Datacenter.class);
    private RepGroup mockedRepGroup1 = createMock(RepGroup.class);
    private RepNode mockedRepNode1 = createMock(RepNode.class);
    private RepNode mockedRepNode2 = createMock(RepNode.class);
    private ArbNode mockedArbNode1 = createMock(ArbNode.class);
    private RepGroupMap mockedRepGroupMap = createMock(RepGroupMap.class);

    private StorageNode mockedStorageNode = createMock(StorageNode.class);
    private RegistryUtils mockedRegistryUtils = createMock(RegistryUtils.class);
    private StorageNodeAgentAPI mockedStorageNodeAgentAPI =
                                    createMock(StorageNodeAgentAPI.class);
    private StorageNodeStatus mockedStorageNodeStatus =
                                  createMock(StorageNodeStatus.class);

    private boolean usingThreads = false;
    private String hostname = "localhost";
    private String haHostname = hostname;
    private String jeHelperHosts = hostname + ":" + REGISTRY_PORT;
    private String storeNameBase = "kvstore" + "_" + THIS_CLASS_NAME;
    private String rootDirBase =
                       TMP_DIR + F_SEP + "KV_ROOT_DIR" + "_" + THIS_CLASS_NAME;
    private String userAliasBase = "user_alias" + "_" + THIS_CLASS_NAME;
    private String storeName;
    private String rootDir;
    private String userAlias;
    private List<Datacenter> datacenterList = Arrays.asList(mockedDatacenter1);
    private DatacenterId datacenterId1 = new DatacenterId(1);
    private RepGroupId repGroupId1 = new RepGroupId(1);
    private List<RepGroup> repGroupList = Arrays.asList(mockedRepGroup1);
    private List<RepNode> repNodeList =
                              Arrays.asList(mockedRepNode1, mockedRepNode2);
    private List<ArbNode> arbNodeList = Arrays.asList(mockedArbNode1);
    private RepNodeId repNodeId1 = new RepNodeId(repGroupId1.getGroupId(), 1);
    private RepNodeId repNodeId2 = new RepNodeId(repGroupId1.getGroupId(), 1);
    private ArbNodeId arbNodeId1 = new ArbNodeId(repGroupId1.getGroupId(), 1);
    private Set<RepGroupId> repGroupIdSet =
                                new HashSet<>(Arrays.asList(repGroupId1));
    private Set<RepNodeId> repNodeIdSet =
                           new HashSet<>(Arrays.asList(repNodeId1, repNodeId2));
    private Set<ArbNodeId> arbNodeIdSet =
                               new HashSet<>(Arrays.asList(arbNodeId1));
    private ReplicatedEnvironment.State nodeStateMaster =
                                            ReplicatedEnvironment.State.MASTER;
    private ReplicatedEnvironment.State nodeStateReplica =
                                            ReplicatedEnvironment.State.REPLICA;
    private List<String> jeDbNamesList = Arrays.asList("jeDbName_1");
    private AdminId adminId1 = new AdminId(1);
    private AdminId adminId2 = new AdminId(2);
    private StorageNodeId storageNodeId = new StorageNodeId(1);
    private Set<AdminId> adminIdSet =
                             new HashSet<>(Arrays.asList(adminId1, adminId2));
    private List<StorageNodeId> storageNodeIdList =
                                    Arrays.asList(storageNodeId);
    private Set<StorageNode> storageNodeSet =
                                new HashSet<>(Arrays.asList(mockedStorageNode));
    private List<AdminParams> adminParamsCollection =
                Arrays.asList(mockedAdmin1Params, mockedAdmin2Params);

    private ParameterMap globalParameterMap = new ParameterMap();
    private ParameterMap storageNodeParameterMap = new ParameterMap();
    private ParameterMap adminParameterMap = new ParameterMap();
    private ParameterMap securityParameterMap = new ParameterMap();
    private ParameterMap storageDirMap = new ParameterMap();
    private ParameterMap rnLogDirMap = new ParameterMap();
    private DatacenterMap datacenterMap = new DatacenterMap(mockedTopology);
    private StorageNodeMap storageNodeMap = new StorageNodeMap(mockedTopology);
    private PartitionMap partitionMap = new PartitionMap(mockedTopology);

    private SSLTransport rmiPolicyBuilder = new SSLTransport();
    private RMISocketPolicy rmiPolicy;

    private Properties props;
    private EnvironmentConfig envConfig;

    private String methodName;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        globalParameterMap.setParameter(ParameterState.COMMON_STORENAME,
                                        storeName);
        storageNodeParameterMap.setParameter(
            ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS,
            rootDir, ParameterState.BOOTSTRAP_TYPE, true);
        adminParameterMap.setParameter(
            ParameterState.JE_HELPER_HOSTS, jeHelperHosts);
        securityParameterMap.setParameter(
            ParameterState.SEC_KEYSTORE_PWD_ALIAS, userAlias);
        rmiPolicy =
            rmiPolicyBuilder.makeSocketPolicy(new SecurityParams(),
                                              securityParameterMap, logger);
        props = new ParameterUtils(adminParameterMap).createProperties(
                                                          true, true, 0L);
        envConfig = new EnvironmentConfig(props);
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        envConfig.setCacheSize(0L);
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_PREFIX,
        adminId1.getFullName());

        methodName = testName.getMethodName();
        logger.fine("\n\n--- ENTERED " + methodName + " ---\n");

        initTestCaseInfo(methodName);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testCheckStoreVersionTrue() throws Exception {

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final KVVersion requiredVersion = admin.getStoreVersion();
        final boolean storeVersionOk =
            admin.checkStoreVersion(requiredVersion);
        assertTrue(storeVersionOk);
    }

    @Test
    public void testCheckStoreVersionFalse() throws Exception {

        final KVVersion requiredVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean storeVersionOk =
            admin.checkStoreVersion(requiredVersion);
        assertTrue(!storeVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionBasicCoverageTrue() throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(requiredVersion);
        assertTrue(adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionBasicCoverageFalse()
                    throws Exception {

        final KVVersion requiredVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(requiredVersion);
        assertTrue(!adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionFailedShardCoverageTrue()
                    throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(
                requiredVersion, repGroupId1);
        assertTrue(adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionFailedShardCoverageFalse()
                    throws Exception {

        final KVVersion requiredVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(
                requiredVersion, repGroupId1);
        assertTrue(!adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionAdminVictimSnFailedTrue()
                    throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean failedSN = true;
        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(
                requiredVersion, adminId1, failedSN);
        assertTrue(adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionAdminVictimSnFailedFalse()
                    throws Exception {

        final KVVersion requiredVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean failedSN = true;
        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(
                requiredVersion, adminId1, failedSN);
        assertTrue(!adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionAdminVictimSnNotFailedTrue()
                    throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean failedSN = false;
        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(
                requiredVersion, adminId1, failedSN);
        assertTrue(adminGroupVersionOk);
    }

    @Test
    public void testCheckAdminGroupVersionAdminVictimSnNotFailedFalse()
                    throws Exception {

        final KVVersion requiredVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean failedSN = false;
        final boolean adminGroupVersionOk =
            admin.checkAdminGroupVersion(
                requiredVersion, adminId1, failedSN);
        assertTrue(!adminGroupVersionOk);
    }

    @Test
    public void testGetMasterWebServiceAddress() throws Exception {

        final URI expectedAddress =
            new URI("http", null, hostname, ADMIN_WEB_SERVICE_PORT,
                    null, null, null);

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final URI retUri = admin.getMasterWebServiceAddress();
        assertEquals(expectedAddress, retUri);
    }

    @Test
    public void testGetMasterWebServiceAddressNullMasterId() throws Exception {

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 3L, null);

        final URI retUri = admin.getMasterWebServiceAddress();
        assertNull("With null masterId, expected null return value from " +
                   "Admin.getMasterWebServiceAddress(), but received " +
                   "non-null URI = " + retUri, retUri);
    }

    @Test
    public void testGetMasterWebServiceAddressNonPositiveWebPort()
                    throws Exception {

        initAdminMocks(0, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final URI retUri = admin.getMasterWebServiceAddress();
        assertNull(methodName + ": with non-positive Admin Web Port, " +
                   "expected null return value from " +
                   "Admin.getMasterWebServiceAddress(), but " +
                   "received non-null URI = " + retUri, retUri);
    }

    @Test
    public void testGetMasterWebServiceAddressUriSyntaxException()
                    throws Exception {

        /* Section 2.3 of RFC2396 says "~" and "_" not allowed in hostname */
        final String invalidHostname = "xxxx~yyyy_zzzz";
        initAdminMocks(ADMIN_WEB_SERVICE_PORT, invalidHostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        try {
            final URI retUri = admin.getMasterWebServiceAddress();
            fail(methodName + ": expected NonfatalAssertionException " +
                "(URISyntaxException) but Admin.getMasterWebServiceAddress() " +
                "returned successfully [returned value = " + retUri + "]");
        } catch (NonfatalAssertionException e) /* CHECKSTYLE:OFF */ {
            /* expected */
        }/* CHECKSTYLE:ON */
    }

    @Test
    public void testGetMasterWebServiceAddressIsSecure() throws Exception {

        final URI expectedAddress =
            new URI("https", null, hostname, ADMIN_WEB_SERVICE_PORT,
                    null, null, null);

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, true);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final URI retUri = admin.getMasterWebServiceAddress();
        assertEquals(expectedAddress, retUri);
    }

    @Test
    public void testGetAdminHighestVersion() throws Exception {

        /*
         * Using a version with a minor component greater than the
         * minor component of the KVVersion.CURRENT_VERSION will cause
         * Admin.getAdminHighestVersion to not only exercise the code
         * that retrieves the version from StorageNode, but also the
         * that compares minor versions. This should result in full
         * code coverage of Admin.getAdminHighestVersion.
         */
        final KVVersion expectedVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final KVVersion retVersion =
            admin.getAdminHighestVersion();
        assertEquals(expectedVersion, retVersion);
    }

    @Test
    public void testGetOtherAdminVersions() throws Exception {

        final Map<AdminId, KVVersion> expectedVersionMap = new HashMap<>();
        expectedVersionMap.put(adminId2, NEXT_KV_MINOR_VERSION);

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final Map<AdminId, KVVersion> retVersionMap =
            admin.getOtherAdminVersions();
        assertEquals(expectedVersionMap, retVersionMap);
    }

    @Test
    public void testGetOtherAdminVersionsException() throws Exception {

        final Map<AdminId, KVVersion> expectedVersionMap = null;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster,
            0L, adminId1, false, mockedAdminStoresNullParams);

        final Map<AdminId, KVVersion> retVersionMap =
                                          admin.getOtherAdminVersions();
        assertEquals(expectedVersionMap, retVersionMap);
    }

    @Test
    public void testCheckAdminMasterVersionCurrentMasterEqualsAdminId()
                    throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean adminMasterVersionOk =
            admin.checkAdminMasterVersion(requiredVersion);
        assertTrue(adminMasterVersionOk);
    }

    @Test
    public void testCheckAdminMasterVersionCurrentMasterNull()
                    throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 3L, null);

        try {
            final boolean adminMasterVersionOk =
                admin.checkAdminMasterVersion(requiredVersion);
            fail(methodName + ": expected AdminFaultException " +
                "(NonfatalAssertionException) but " +
                "Admin.checkAdminMasterVersion() returned successfully " +
                "[returned value = " + adminMasterVersionOk + "]");
        } catch (AdminFaultException e) /* CHECKSTYLE:OFF */ {
            /* expected */
        }/* CHECKSTYLE:ON */
    }

    @Test
    public void testCheckAdminMasterVersionCurrentMasterLessThanRequired()
                    throws Exception {

        final KVVersion requiredVersion = NEXT_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final boolean adminMasterVersionOk =
                          admin.checkAdminMasterVersion(requiredVersion);
        assertTrue(!adminMasterVersionOk);
    }

    /**
     * Invokes the Admin.checkAdminMasterAdminVersion method with a
     * configuration that results in the primary path of the
     * Admin.getSNsVersion method being exercised.
     */
    @Test
    public void testGetSNsVersionBasicCoverage() throws Exception {

        final KVVersion requiredVersion = KVVersion.CURRENT_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId2);

        final String curSnSoftwareVersion =
            storageNodeParameterMap.get(
                ParameterState.SN_SOFTWARE_VERSION).asString();
        try {
            admin.setParameters(mockedParameters);
            storageNodeParameterMap.setParameter(
                ParameterState.SN_SOFTWARE_VERSION,
                requiredVersion.getVersionString());

            final boolean adminMasterVersionOk =
                admin.checkAdminMasterVersion(requiredVersion);
            assertTrue(adminMasterVersionOk);
        } finally {
            /* Reset configuration to original values. */
            admin.setParameters(null);
            storageNodeParameterMap.setParameter(
                ParameterState.SN_SOFTWARE_VERSION, curSnSoftwareVersion);
        }
    }

    /**
     * Invokes the Admin.checkAdminMasterAdminVersion method with a
     * configuration that results in exercising the if block that checks
     * for a lesser minor version and downgrades if the minor version is
     * less than the required version.
     */
    @Test
    public void testGetSNsVersionCoverMinorVersionComparison()
                    throws Exception {

        final KVVersion requiredVersion = PREV_KV_MINOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId2);

        final String curSnSoftwareVersion =
            storageNodeParameterMap.get(
                ParameterState.SN_SOFTWARE_VERSION).asString();
        try {
            admin.setParameters(mockedParameters);
            storageNodeParameterMap.setParameter(
                ParameterState.SN_SOFTWARE_VERSION,
                requiredVersion.getVersionString());

            final boolean adminMasterVersionOk =
                admin.checkAdminMasterVersion(requiredVersion);
            assertTrue(adminMasterVersionOk);
        } finally {
            /* Reset configuration to original values. */
            admin.setParameters(null);
            storageNodeParameterMap.setParameter(
                ParameterState.SN_SOFTWARE_VERSION, curSnSoftwareVersion);
        }
    }

    /**
     * Invokes the Admin.checkAdminMasterAdminVersion method with a
     * configuration that results in covering the path of the
     * Admin.getSNsVersion method that throws a FaultException because
     * version of the SN is too old.
     */
    @Test
    public void testGetSNsVersionLessThanCurPrereqThrowAdminFaultException()
                    throws Exception {

        final KVVersion oldSnVersion = OLD_KV_MAJOR_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        /* Note: mockedAdmin1Params (which has adminId1) is used when
         *       creating the Admin to be tested, but a state change
         *       to replica is initiated and the master is set to
         *       adminId2. This is so that the 'else block' in
         *       in checkAdminMasterVersion (when the currentMaster and
         *       the current adminId are not equal) is covered by this
         *       test case.
         *
         *       The version of the SN is also set to a version old
         *       enough to guarantee that getSNsVerion (called by
         *       checkAdminMasterVersion) will throw an AdminFaultException
         *       because the SN is at a version which does not meet the
         *       current prerequisite and must be upgraded to a version
         *       that does.
         */
        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventReplica,
            3L, adminId2, true);

        final String curSnSoftwareVersion =
            storageNodeParameterMap.get(
                ParameterState.SN_SOFTWARE_VERSION).asString();
        try {
            admin.setParameters(mockedParameters);
            storageNodeParameterMap.setParameter(
                ParameterState.SN_SOFTWARE_VERSION,
                oldSnVersion.getVersionString());
            try {
                final boolean adminMasterVersionOk =
                    admin.checkAdminMasterVersion(oldSnVersion);
                fail(methodName + ": expected AdminFaultException " +
                    "(NonfatalAssertionException) but " +
                    "Admin.checkAdminMasterVersion() returned successfully " +
                    "[returned value = " + adminMasterVersionOk + "]");
            } catch (AdminFaultException e) /* CHECKSTYLE:OFF */ {
                /* expected */
            }/* CHECKSTYLE:ON */
        } finally {
            /* Reset configuration to original values. */
            admin.setParameters(null);
            storageNodeParameterMap.setParameter(
                ParameterState.SN_SOFTWARE_VERSION, curSnSoftwareVersion);
        }
    }

    @Test
    public void testCheckMRAgentVersionBasicCoverage() throws Exception {

        /* Without an actual agent, the max version will ne the prereq */
        final KVVersion expectedMinMrAgentVersion =
                                        KVVersion.PREREQUISITE_VERSION;

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final KVVersion retVersion =
            admin.getMRTServiceManager().getMaxVersion();
        assertEquals(expectedMinMrAgentVersion, retVersion);
    }

    @Test
    public void testValidateTopology() throws Exception {

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final String candidateName = "testCandidateName";

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final String retVal = admin.validateTopology(candidateName, (short) 1);
        assertNotNull(retVal);
    }

    @Test
    public void testValidateTopologyCliV1() throws Exception {

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final String candidateName = "testCandidateName";
        final String retValPrefix =
            "Validation for topology candidate \"" + candidateName + "\":";

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final String retVal =
            admin.validateTopology(candidateName,
                oracle.kv.impl.util.SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        assertTrue(retVal.startsWith(retValPrefix));
    }

    @Test
    public void testValidateTopologyNullCandidateName() throws Exception {

        initAdminMocks(ADMIN_WEB_SERVICE_PORT, hostname, false);

        final String candidateName = null;

        final Admin admin = createTestAdmin(
            mockedAdmin1Params, mockedStateChangeEventMaster, 0L, adminId1);

        final String retVal = admin.validateTopology(candidateName, (short) 1);
        assertNotNull(retVal);
    }

    private void initTestCaseInfo(final String testCaseName) {

        storeName = storeNameBase + "_" + testCaseName;
        rootDir = rootDirBase + "_" + testCaseName;
        userAlias = userAliasBase + "_" + testCaseName;

        final String envConfigDirName = rootDir + "/log";
        final File envConfigDirNameFd = new File(envConfigDirName);
        envConfigDirNameFd.mkdirs();
        envConfig.setConfigParam(
            EnvironmentConfig.FILE_LOGGING_DIRECTORY, envConfigDirName);
    }

    private void initAdminMocks(final int adminWebPort,
                                final String inputHostname,
                                final boolean isSecure) {

        expect(mockedBootstrapParams.getStoreName())
            .andReturn(storeName).anyTimes();
        expect(mockedBootstrapParams.getId())
            .andReturn(1).anyTimes();
        expect(mockedBootstrapParams.getUserExternalAuth())
            .andReturn("NONE").anyTimes();
        expect(mockedBootstrapParams.getHostname())
            .andReturn(inputHostname).anyTimes();
        expect(mockedBootstrapParams.getRegistryPort())
            .andReturn(REGISTRY_PORT).anyTimes();
        expect(mockedBootstrapParams.getRootdir())
            .andReturn(rootDir).anyTimes();
        expect(mockedBootstrapParams.getAdminDirMap())
            .andReturn(new ParameterMap()).anyTimes();
        expect(mockedBootstrapParams.getHAHostname())
            .andReturn(haHostname).anyTimes();
        expect(mockedBootstrapParams.getHAPortRange())
            .andReturn(HA_PORT_RANGE).anyTimes();
        expect(mockedBootstrapParams.getServicePortRange())
            .andReturn(SERVICE_PORT_RANGE).anyTimes();
        expect(mockedBootstrapParams.getAdminWebServicePort())
            .andReturn(adminWebPort).anyTimes();
        replay(mockedBootstrapParams);

        expect(mockedSecurityParams.isSecure())
            .andReturn(isSecure).anyTimes();
        expect(mockedSecurityParams.getRMISocketPolicy())
            .andReturn(rmiPolicy).anyTimes();
        expect(mockedSecurityParams.getKeystorePasswordAlias())
            .andReturn(userAlias).anyTimes();
        expect(mockedSecurityParams.getJEHAProperties())
            .andReturn(new Properties()).anyTimes();
        expect(mockedSecurityParams.allTransportSSLDisabled())
            .andReturn(false).anyTimes();
        expect(mockedSecurityParams.getKerberosConfFile())
            .andReturn(null).anyTimes();
        mockedSecurityParams.initRMISocketPolicies(logger);
        expectLastCall().anyTimes();
        replay(mockedSecurityParams);

        expect(mockedGlobalParams.getKVStoreName())
            .andReturn(storeName).anyTimes();
        expect(mockedGlobalParams.getMap())
            .andReturn(globalParameterMap).anyTimes();
        replay(mockedGlobalParams);

        expect(mockedLoadParameters.getMap(ParameterState.GLOBAL_TYPE))
            .andReturn(globalParameterMap).anyTimes();
        expect(mockedLoadParameters.getMap(ParameterState.SNA_TYPE))
            .andReturn(storageNodeParameterMap).anyTimes();
        expect(mockedLoadParameters.getMap(
                   ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS))
            .andReturn(storageNodeParameterMap).anyTimes();
        replay(mockedLoadParameters);

        expect(mockedAdmin1Params.getAdminId())
            .andReturn(adminId1).anyTimes();
        expect(mockedAdmin1Params.getLogFileLimit())
            .andReturn(1000).anyTimes();
        expect(mockedAdmin1Params.getLogFileCount())
            .andReturn(3).anyTimes();
        expect(mockedAdmin1Params.createCSV())
            .andReturn(true).anyTimes();
        expect(mockedAdmin1Params.getEventRecorderPollingIntervalMs())
            .andReturn(5000L).anyTimes();
        expect(mockedAdmin1Params.getType())
            .andReturn(AdminType.valueOf(adminParameterMap.getOrDefault(
                ParameterState.AP_TYPE).asString())).anyTimes();
        expect(mockedAdmin1Params.getMap())
            .andReturn(adminParameterMap).anyTimes();
        expect(mockedAdmin1Params.getElectableGroupSizeOverride())
            .andReturn(adminParameterMap.getOrDefault(
                ParameterState.COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE)
                    .asInt()).anyTimes();
        expect(mockedAdmin1Params.getResetRepGroup())
            .andReturn(adminParameterMap.getOrDefault(
                ParameterState.COMMON_RESET_REP_GROUP).asBoolean()).anyTimes();
        expect(mockedAdmin1Params.getStorageNodeId())
            .andReturn(storageNodeId).anyTimes();
        replay(mockedAdmin1Params);

        expect(mockedAdmin2Params.getAdminId())
            .andReturn(adminId2).anyTimes();
        expect(mockedAdmin2Params.getLogFileLimit())
            .andReturn(1000).anyTimes();
        expect(mockedAdmin2Params.getLogFileCount())
            .andReturn(3).anyTimes();
        expect(mockedAdmin2Params.createCSV())
            .andReturn(true).anyTimes();
        expect(mockedAdmin2Params.getEventRecorderPollingIntervalMs())
            .andReturn(5000L).anyTimes();
        expect(mockedAdmin2Params.getType())
            .andReturn(AdminType.valueOf(adminParameterMap.getOrDefault(
                ParameterState.AP_TYPE).asString())).anyTimes();
        expect(mockedAdmin2Params.getMap())
            .andReturn(adminParameterMap).anyTimes();
        expect(mockedAdmin2Params.getElectableGroupSizeOverride())
            .andReturn(adminParameterMap.getOrDefault(
                ParameterState.COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE)
                    .asInt()).anyTimes();
        expect(mockedAdmin2Params.getResetRepGroup())
            .andReturn(adminParameterMap.getOrDefault(
                ParameterState.COMMON_RESET_REP_GROUP).asBoolean()).anyTimes();
        expect(mockedAdmin2Params.getStorageNodeId())
            .andReturn(storageNodeId).anyTimes();
        replay(mockedAdmin2Params);

        expect(mockedParameters.get(adminId1))
            .andReturn(mockedAdmin1Params).anyTimes();
        expect(mockedParameters.get(adminId2))
            .andReturn(mockedAdmin2Params).anyTimes();
         expect(mockedParameters.get(storageNodeId))
            .andReturn(mockedStorageNodeParams).anyTimes();
         expect(mockedParameters.getAdminIds())
            .andReturn(adminIdSet).anyTimes();
         expect(mockedParameters.getAdminParams())
            .andReturn(adminParamsCollection).anyTimes();
        expect(mockedParameters.get(repNodeId1))
            .andReturn(mockedRepNodeParams).anyTimes();
        expect(mockedParameters.get(arbNodeId1))
            .andReturn(mockedArbNodeParams).anyTimes();
        replay(mockedParameters);

        expect(mockedAdminStores.getParameters(anyObject(Transaction.class)))
            .andReturn(mockedParameters).anyTimes();
        expect(mockedAdminStores.getTopology(mockedTransaction))
            .andReturn(mockedTopology).anyTimes();
        expect(mockedAdminStores.getTopologyStore())
            .andReturn(mockedTopologyStore).anyTimes();
        replay(mockedAdminStores);

        expect(mockedAdminStoresNullParams
            .getParameters(anyObject(Transaction.class)))
                .andReturn(null).anyTimes();
        expect(mockedAdminStoresNullParams.getTopology(mockedTransaction))
            .andReturn(mockedTopology).anyTimes();
        expect(mockedAdminStoresNullParams.getTopologyStore())
            .andReturn(mockedTopologyStore).anyTimes();
        replay(mockedAdminStoresNullParams);

        expect(mockedStorageNodeParams.getRootDirPath())
            .andReturn(rootDir).anyTimes();
        expect(mockedStorageNodeParams.getAdminDirMap())
            .andReturn(storageNodeParameterMap).anyTimes();
        expect(mockedStorageNodeParams.getStorageNodeId())
            .andReturn(storageNodeId).anyTimes();
        expect(mockedStorageNodeParams.getLogFileLimit())
            .andReturn(1000).anyTimes();
        expect(mockedStorageNodeParams.getLogFileCount())
            .andReturn(3).anyTimes();
        expect(mockedStorageNodeParams.getLogFileCompression())
            .andReturn(true).anyTimes();
        expect(mockedStorageNodeParams.getAdminWebPort())
            .andReturn(adminWebPort).anyTimes();
        expect(mockedStorageNodeParams.getHostname())
            .andReturn(inputHostname).anyTimes();
        expect(mockedStorageNodeParams.getCapacity())
            .andReturn(1).anyTimes();
        expect(mockedStorageNodeParams.getStorageDirMap())
            .andReturn(storageDirMap).anyTimes();
        expect(mockedStorageNodeParams.getRootDirSize())
            .andReturn(0L).anyTimes();
        expect(mockedStorageNodeParams.getRNLogDirMap())
            .andReturn(rnLogDirMap).anyTimes();
        expect(mockedStorageNodeParams.getMemoryMB())
            .andReturn(0).anyTimes();
        expect(mockedStorageNodeParams.getMap())
            .andReturn(storageNodeParameterMap).anyTimes();
        replay(mockedStorageNodeParams);

        expect(mockedRepNodeParams.getStorageDirectoryPath())
            .andReturn(rootDir).anyTimes();
        expect(mockedRepNodeParams.getJEHelperHosts())
            .andReturn(jeHelperHosts).anyTimes();
        expect(mockedRepNodeParams.getJENodeHostPort())
            .andReturn(Integer.toString(REGISTRY_PORT)).anyTimes();
        expect(mockedRepNodeParams.getNodeType())
            .andReturn(NodeType.ELECTABLE).anyTimes();
        replay(mockedRepNodeParams);

        expect(mockedArbNodeParams.getJEHelperHosts())
            .andReturn(jeHelperHosts).anyTimes();
        expect(mockedArbNodeParams.getJENodeHostPort())
            .andReturn(Integer.toString(REGISTRY_PORT)).anyTimes();
        replay(mockedArbNodeParams);

        expect(mockedReplicatedEnvironment.getConfig())
            .andReturn(envConfig).anyTimes();
        expect(mockedReplicatedEnvironment.getState())
            .andReturn(nodeStateMaster).anyTimes();
        expect(mockedReplicatedEnvironment
                  .beginTransaction(anyObject(Transaction.class),
                                    anyObject(TransactionConfig.class)))
            .andReturn(mockedTransaction).anyTimes();
        expect(mockedReplicatedEnvironment.getDatabaseNames())
            .andReturn(jeDbNamesList).anyTimes();
        expect(mockedReplicatedEnvironment.isValid())
            .andReturn(true).anyTimes();
        replay(mockedReplicatedEnvironment);

        expect(mockedStateChangeEventMaster.getState())
            .andReturn(nodeStateMaster).anyTimes();
        expect(mockedStateChangeEventMaster.getEventTime())
            .andReturn(System.currentTimeMillis()).anyTimes();
        expect(mockedStateChangeEventMaster.getMasterNodeName())
            .andReturn("1").anyTimes();
        replay(mockedStateChangeEventMaster);

        expect(mockedStateChangeEventReplica.getState())
            .andReturn(nodeStateReplica).anyTimes();
        expect(mockedStateChangeEventReplica.getEventTime())
            .andReturn(System.currentTimeMillis()).anyTimes();
        expect(mockedStateChangeEventReplica.getMasterNodeName())
            .andReturn("1").anyTimes();
        replay(mockedStateChangeEventReplica);

        expect(mockedTransaction.getEnvironment())
            .andReturn(mockedReplicatedEnvironment).anyTimes();

        expect(mockedTransaction.getState())
                .andReturn(Transaction.State.OPEN).anyTimes();

        mockedTransaction.commit();
        expectLastCall().anyTimes();

        mockedTransaction.abort();
        expectLastCall().anyTimes();

        replay(mockedTransaction);

        expect(mockedTopology.get(storageNodeId))
            .andReturn(mockedStorageNode).anyTimes();
        expect(mockedTopology.getKVStoreName())
            .andReturn(storeName).anyTimes();
        expect(mockedTopology.getSortedDatacenters())
            .andReturn(datacenterList).anyTimes();
        expect(mockedTopology.getSortedRepNodes())
            .andReturn(repNodeList).anyTimes();
        expect(mockedTopology.getRepGroupIds())
            .andReturn(repGroupIdSet).anyTimes();
        expect(mockedTopology.get(datacenterId1))
            .andReturn(mockedDatacenter1).anyTimes();
        expect(mockedTopology.get(repGroupId1))
            .andReturn(mockedRepGroup1).anyTimes();
        expect(mockedTopology.getStorageNodeIds())
            .andReturn(storageNodeIdList).anyTimes();
        expect(mockedTopology.getDatacenterMap())
            .andReturn(datacenterMap).anyTimes();
        expect(mockedTopology.getStorageNodes(null))
            .andReturn(storageNodeSet).anyTimes();
        expect(mockedTopology.getRepNodeIds())
            .andReturn(repNodeIdSet).anyTimes();
        expect(mockedTopology.getArbNodeIds())
            .andReturn(arbNodeIdSet).anyTimes();
        expect(mockedTopology.get(arbNodeId1))
            .andReturn(mockedArbNode1).anyTimes();
        expect(mockedTopology.getRepGroupMap())
            .andReturn(mockedRepGroupMap).anyTimes();
        expect(mockedTopology.getStorageNodeMap())
            .andReturn(storageNodeMap).anyTimes();
        expect(mockedTopology.getPartitionMap())
            .andReturn(partitionMap).anyTimes();
        expect(mockedTopology.get(repNodeId1))
            .andReturn(mockedRepNode1).anyTimes();
        expect(mockedTopology.getDatacenter(storageNodeId))
            .andReturn(mockedDatacenter1).anyTimes();
        expect(mockedTopology.getDatacenterId(storageNodeId))
            .andReturn(datacenterId1).anyTimes();
        expect(mockedTopology.getHostedRepNodeIds(storageNodeId))
            .andReturn(repNodeIdSet).anyTimes();
        replay(mockedTopology);

        expect(mockedDatacenter1.getResourceId())
            .andReturn(datacenterId1).anyTimes();
        expect(mockedDatacenter1.getRepFactor())
            .andReturn(3).anyTimes();
        expect(mockedDatacenter1.getDatacenterType())
            .andReturn(DatacenterType.PRIMARY).anyTimes();
        replay(mockedDatacenter1);

        expect(mockedTopologyStore.getCandidate(anyObject(Transaction.class),
                                                anyObject(String.class)))
            .andReturn(mockedTopologyCandidate).anyTimes();
        replay(mockedTopologyStore);

        expect(mockedTopologyCandidate.getTopology())
            .andReturn(mockedTopology).anyTimes();
        replay(mockedTopologyCandidate);

        expect(mockedStorageNode.getHostname())
            .andReturn(hostname).anyTimes();
        expect(mockedStorageNode.getRegistryPort())
            .andReturn(REGISTRY_PORT).anyTimes();
        expect(mockedStorageNode.getDatacenterId())
            .andReturn(datacenterId1).anyTimes();
        expect(mockedStorageNode.getResourceId())
            .andReturn(storageNodeId).anyTimes();
        replay(mockedStorageNode);

        expect(mockedRepNode1.getStorageNodeId())
            .andReturn(storageNodeId).anyTimes();
        expect(mockedRepNode1.getRepGroupId())
            .andReturn(repGroupId1).anyTimes();
        expect(mockedRepNode1.getResourceId())
            .andReturn(repNodeId1).anyTimes();
        replay(mockedRepNode1);

        expect(mockedRepGroup1.getRepNodes())
            .andReturn(repNodeList).anyTimes();
        expect(mockedRepGroup1.getArbNodes())
            .andReturn(arbNodeList).anyTimes();
        replay(mockedRepGroup1);

        expect(mockedRepGroupMap.getAll())
            .andReturn(repGroupList).anyTimes();
        expect(mockedRepGroupMap.get(repGroupId1))
            .andReturn(mockedRepGroup1).anyTimes();
        replay(mockedRepGroupMap);

        expect(mockedRepNode2.getStorageNodeId())
            .andReturn(storageNodeId).anyTimes();
        expect(mockedRepNode2.getRepGroupId())
            .andReturn(repGroupId1).anyTimes();
        expect(mockedRepNode2.getResourceId())
            .andReturn(repNodeId2).anyTimes();
        replay(mockedRepNode2);

        expect(mockedArbNode1.getResourceId())
            .andReturn(arbNodeId1).anyTimes();
        expect(mockedArbNode1.getRepGroupId())
            .andReturn(repGroupId1).anyTimes();
        replay(mockedArbNode1);

        try {
            expect(mockedRegistryUtils.getStorageNodeAgent(storageNodeId))
                .andReturn(mockedStorageNodeAgentAPI).anyTimes();
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        replay(mockedRegistryUtils);

        try {
            expect(mockedStorageNodeAgentAPI.ping())
                .andReturn(mockedStorageNodeStatus).anyTimes();
        } catch (Exception e) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
        replay(mockedStorageNodeAgentAPI);

        expect(mockedStorageNodeStatus.getKVVersion())
            .andReturn(NEXT_KV_MINOR_VERSION).anyTimes();
        replay(mockedStorageNodeStatus);

        /* Because this calls StorageNode.getResourceId, must call it
         * AFTER all the mock expectations for mockedStorageNode have
         * been set above.
         */
        storageNodeMap.put(mockedStorageNode);
    }

    private Admin createTestAdmin(final AdminParams adminParams,
                                    final StateChangeEvent stateChangeEvent,
                                    final long nSecsDelay,
                                    final AdminId adminId) {

        return createTestAdmin(adminParams,
                               stateChangeEvent,
                               nSecsDelay,
                               adminId,
                               false,
                               mockedAdminStores);
    }

    private Admin createTestAdmin(final AdminParams adminParams,
                                  final StateChangeEvent stateChangeEvent,
                                  final long nSecsDelay,
                                  final AdminId adminId,
                                  final boolean guaranteeNotEqual) {

        return createTestAdmin(adminParams,
                               stateChangeEvent,
                               nSecsDelay,
                               adminId,
                               guaranteeNotEqual,
                               mockedAdminStores);
    }

    private Admin createTestAdmin(final AdminParams adminParams,
                                  final StateChangeEvent stateChangeEvent,
                                  final long nSecsDelay,
                                  final AdminId adminId,
                                  final boolean guaranteeNotEqual,
                                  final AdminStores adminStores) {

        final AdminServiceParams adminServiceParams =
            new AdminServiceParams(mockedSecurityParams, mockedGlobalParams,
                                   mockedStorageNodeParams, adminParams);
        final AdminService adminService =
            new AdminService(
                mockedBootstrapParams, mockedSecurityParams, usingThreads);
        final Admin admin =
            new TestAdmin(
                adminServiceParams, adminService,
                mockedReplicatedEnvironment, mockedStartupStatus,
                adminStores, mockedRegistryUtils);
        admin.stateChange(stateChangeEvent);
        if (nSecsDelay > 0L) {
            /* Delay so state change can complete before setting masterId. */
            try {
                Thread.sleep(nSecsDelay * 1000L);
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            }/* CHECKSTYLE:ON */
        }
        AdminId newMasterId = adminId;
        if (guaranteeNotEqual) {
           final AdminId curAdminId = admin.getAdminId();
           if (curAdminId.equals(newMasterId) && adminId1.equals(newMasterId)) {
               newMasterId = adminId2;
           }
        }
        admin.setMasterId(newMasterId);
        return admin;
    }

    /*
     * Special sub-class of the Admin that is used by only this test class.
     * This sub-class allows the necessary mocked/simulated behavior to be
     * injected into the Admin when running the test cases of this class.
     */
    static class TestAdmin extends Admin {

        private RegistryUtils registryUtils;

        TestAdmin(final AdminServiceParams params,
                  final AdminService owner,
                  final ReplicatedEnvironment testEnvironment,
                  final StartupStatus testStartupStatus,
                  final AdminStores testAdminStores,
                  final RegistryUtils registryUtils) {

            super(params, owner, testEnvironment,
                  testStartupStatus, testAdminStores);

            this.registryUtils = registryUtils;
        }

        @Override
        RegistryUtils createRegistryUtils() {
            return registryUtils;
        }
    }
}
