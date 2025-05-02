/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManagerInterface;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.TestUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Utilities for creating kvstores and their components, using CommandService
 * interfaces. Geared toward admin-centric tests which are content to use
 * the public CommandService APIs, and which don't need to delve deep into
 * Admin internals.
 *
 * Used by both unit and standalone tests, and should not rely on any Junit
 * classes, including classes such as StorageNodeTestBase.
 *
 * The expected usage pattern is:
 *
 * - Call AdminUtils.bootstrapStore with the number of installed but nascent
 * SNS, the name of the initial datacenter, repfactor, and it will configure a
 * store with that datacenter, one SN, and an admin instance. It will return a
 * SysAdminInfo struct, which the test class holds onto to, mimic the
 * information kept by the system administrator.
 *
 * - From this point on, the test uses the CommandService and the SNSet
 * returned in the SysAdminInfo. The former is the equivalent of the using
 * the CLI, and while SNSet mimics the sys admin's knowledge of where the SNs
 * are. The test might deploy more SNs, via SNSet.deploy, and then use the
 * CommandService directly, to execute the desired plans.
 *
 * Example:
 *
 *     // Bootstrap the Admin, the first DC, and the first SN
 *     sysAdminInfo =
 *         AdminUtils.bootstrapStore(kvstoreName, 10, "DataCenterA", 3);
 *
 *     // Deploy more SNs on DataCenterA
 *     CommandServiceAPI cs = sysAdminInfo.getCommandService();
 *     SNSet snSet = sysAdminInfo.getSNSet();
 *     snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), 1, 2);
 *
 *     cs.createTopology("first", Parameters.DEFAULT_POOL_NAME, 100);
 *     int firstPlan = cs.createDeployTopologyPlan("tryThis", "first");
 *     cs.approvePlan(firstPlan);
 *     cs.executePlan(firstPlan);
 *
 * The user should be sure to shut down the SNS this way at the end of the test
 *     snSet.shutdown();
 */
public class AdminUtils {

    public static String HOSTNAME = "localhost";

    /**
     * Configure and deploy the very first SN, datacenter, and Admin,
     * to bootstrap the system.
     * @throws Exception
     * @return the SysAdmin info that lets you obtain a CommandService to
     * represent the AdminService, and the SNSet, to deploy more SNs as desired.
     */
    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String firstDCName,
                                              int firstDCRepFactor)
        throws Exception {

        return bootstrapStore(kvstoreName, numInstalledSNs, firstDCName,
                              firstDCRepFactor, true /* useThreads */,
                              null /* snaMemoryMB */);
    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean masterAffinity)
        throws Exception {

        return bootstrapStore(kvstoreName, numInstalledSNs, null /* snHosts */,
                              firstDCName, firstDCRepFactor,
                              true /* useThreads */, null /* snaMemoryMB */,
                              DatacenterType.PRIMARY, false /* useArbiters */,
                              0 /* rnCachePercent */, masterAffinity);
    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB)
        throws Exception {

        return bootstrapStore(kvstoreName, numInstalledSNs, firstDCName,
                              firstDCRepFactor, useThreads, snaMemoryMB,
                              DatacenterType.PRIMARY);
    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              boolean allowArbiters)
        throws Exception {

        return bootstrapStore(kvstoreName, numInstalledSNs, null, firstDCName,
                              firstDCRepFactor, useThreads, snaMemoryMB,
                              DatacenterType.PRIMARY, allowArbiters);
    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              DatacenterType datacenterType)
        throws Exception {

        return bootstrapStore(kvstoreName, numInstalledSNs, null /* snHosts */,
                              firstDCName, firstDCRepFactor, useThreads,
                              snaMemoryMB, datacenterType);
    }


    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String[] snHosts,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              DatacenterType datacenterType)
        throws Exception {
        return bootstrapStore(kvstoreName, numInstalledSNs, snHosts,
                              firstDCName, firstDCRepFactor, useThreads,
                              snaMemoryMB, datacenterType, false/*useArbiters*/);
    }
    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String[] snHosts,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              DatacenterType datacenterType,
                                              boolean allowArbiters)
        throws Exception {
        return bootstrapStore(kvstoreName, numInstalledSNs, snHosts,
                              firstDCName, firstDCRepFactor, useThreads,
                              snaMemoryMB, datacenterType, allowArbiters, 0,
                              false /* masterAffinity */);
    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String[] snHosts,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              DatacenterType datacenterType,
                                              boolean allowArbiters,
                                              int rnCachePercent)
        throws Exception{
        return bootstrapStore(kvstoreName, numInstalledSNs, snHosts,
                              firstDCName, firstDCRepFactor, useThreads,
                              snaMemoryMB, datacenterType, allowArbiters,
                              rnCachePercent, false /* masterAffinity */);
    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String[] snHosts,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              DatacenterType datacenterType,
                                              boolean allowArbiters,
                                              int rnCachePercent,
                                              boolean masterAffinity)
    throws Exception {
        return bootstrapStore(kvstoreName, numInstalledSNs, snHosts,
                              firstDCName, firstDCRepFactor, useThreads,
                              snaMemoryMB, datacenterType, allowArbiters,
                              rnCachePercent, masterAffinity, false);

    }

    public static SysAdminInfo bootstrapStore(String kvstoreName,
                                              int numInstalledSNs,
                                              String[] snHosts,
                                              String firstDCName,
                                              int firstDCRepFactor,
                                              boolean useThreads,
                                              String snaMemoryMB,
                                              DatacenterType datacenterType,
                                              boolean allowArbiters,
                                              int rnCachePercent,
                                              boolean masterAffinity,
                                              boolean disableAdminThreads)
        throws Exception {

        SNSet snSet =
            new SNSet(kvstoreName, numInstalledSNs, SNSet.START_PORT,
                      SNSet.HA_RANGE, snHosts, useThreads, snaMemoryMB);
        CommandServiceAPI cs = StorageNodeUtils.waitForAdmin
            (HOSTNAME, snSet.getRegistryPort(0), 10);
        assert cs != null: "bootstrap admin is not up";
        cs.configure(kvstoreName);
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.SN_REPNODE_START_WAIT, "300 s");
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        if (rnCachePercent != 0) {
            map.setParameter(ParameterState.RN_CACHE_PERCENT,
                             String.valueOf(rnCachePercent));
        }
        /*
         * Set JE parameters to control JE data verifier.
         */
        String configProps =
            "je.env.runVerifier=" +
            System.getProperty("test.je.env.runVerifier", "true") +
            ";je.env.verifySchedule=" +
            System.getProperty("test.je.env.verifierSchedule", "* * * * *") + ";";
        map.setParameter(ParameterState.JE_MISC, configProps);
        cs.setPolicies(mergeParameterMapDefaults(map));
        SysAdminInfo sysAdminInfo = new SysAdminInfo(cs, snSet);
        deployDatacenter(cs, firstDCName, firstDCRepFactor, datacenterType,
                         sysAdminInfo, allowArbiters, masterAffinity);
        if (snaMemoryMB == null) {
            snaMemoryMB = MB_PER_SN_STRING;
        }
        snSet.deploy(cs, sysAdminInfo.getDCId(firstDCName), snaMemoryMB, 0);
        /* Bootstrap Admin must be PRIMARY, even if dc is a SECONDARY */
        deployAdmin(cs, snSet, 0, AdminType.PRIMARY);

        if (disableAdminThreads) {
            /*
             * Disable admin threads that may create and execute plans.
             */
            map = new ParameterMap();
            map.setType(ParameterState.ADMIN_TYPE);
            map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "false") ;
            map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "false");
            awaitPlanSuccess(
                cs,
                cs.createChangeAllAdminsPlan("changeAdminParams", null, map));
        }
        return sysAdminInfo;
    }

    /**
     * Approve, execute with force=false, and await the completion of a plan,
     * checking for success.
     */
    public static void awaitPlanSuccess(CommandServiceAPI cs, int planId)
        throws RemoteException
    {
        awaitPlanSuccess(cs, planId, false /* force */);
    }

    /**
     * Approve, execute, possibly setting the force flag, and await the
     * completion of a plan, checking for success.
     */
    public static void awaitPlanSuccess(CommandServiceAPI cs,
                                        int planId,
                                        boolean force)
        throws RemoteException
    {
        cs.approvePlan(planId);
        cs.executePlan(planId, force);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    public static void deployDatacenter(CommandServiceAPI cs,
                                        String dcName,
                                        int repFactor,
                                        SysAdminInfo sysAdminInfo)
        throws RemoteException {

        deployDatacenter(cs, dcName, repFactor, DatacenterType.PRIMARY,
                         sysAdminInfo);
    }

    public static void deployDatacenter(CommandServiceAPI cs,
                                        String dcName,
                                        int repFactor,
                                        SysAdminInfo sysAdminInfo,
                                        boolean masterAffinity)
        throws RemoteException {

        deployDatacenter(cs, dcName, repFactor, DatacenterType.PRIMARY,
                         sysAdminInfo, false /* allowArbiters */,
                         masterAffinity);
    }

    public static void deployDatacenter(CommandServiceAPI cs,
                                        String dcName,
                                        int repFactor,
                                        DatacenterType datacenterType,
                                        SysAdminInfo sysAdminInfo)
        throws RemoteException {
        deployDatacenter(cs, dcName, repFactor, datacenterType,
                         sysAdminInfo, false /* allowArbiters */,
                         false /* masterAffinity */);
    }

    public static void deployDatacenter(CommandServiceAPI cs,
                                        String dcName,
                                        int repFactor,
                                        DatacenterType datacenterType,
                                        SysAdminInfo sysAdminInfo,
                                        boolean allowArbiters)
        throws RemoteException {
        deployDatacenter(cs, dcName, repFactor, datacenterType,
                         sysAdminInfo, allowArbiters,
                         false /* masterAffinity */);
    }

    public static void deployDatacenter(CommandServiceAPI cs,
                                        String dcName,
                                        int repFactor,
                                        DatacenterType datacenterType,
                                        SysAdminInfo sysAdminInfo,
                                        boolean allowArbiters,
                                        boolean masterAffinity)
        throws RemoteException {

        awaitPlanSuccess(cs,
                         cs.createDeployDatacenterPlan(
                             "deploy data center", dcName, repFactor,
                             datacenterType, allowArbiters, masterAffinity));

        Topology t = cs.getTopology();
        DatacenterMap dcmap = t.getDatacenterMap();
        Datacenter newDC = null;
        for (Datacenter dc : dcmap.getAll()) {
            if (dc.getName().equals(dcName)) {
                newDC = dc;
                break;
            }
        }

        if (newDC == null) {
            throw new IllegalStateException("Datacenter " + dcName +
                                            " wasn't created");
        }
        sysAdminInfo.putDCId(dcName, newDC.getResourceId());
    }

    /** Remove the specified datacenter */
    public static void removeDatacenter(CommandServiceAPI cs,
                                        String datacenterName,
                                        DatacenterId dcId,
                                        SysAdminInfo sysAdminInfo)
        throws RemoteException {

        awaitPlanSuccess(cs,
                         cs.createRemoveDatacenterPlan(
                             "Remove datacenter " + datacenterName, dcId));
        sysAdminInfo.removeDCId(datacenterName);
    }

    /**
     * Deploy a single Admin instance on this SN.
     * @throws Exception
     */
    public static CommandServiceAPI restartAdmin(SNSet snSet,
                                                 int whichSN)
        throws Exception {

        StorageNodeAgent sna = snSet.getSNA(whichSN);
        AdminParams ap = ConfigUtils.getAdminParams(sna.getKvConfigFile());
        sna.startAdmin(ap);
        return StorageNodeUtils.waitForAdmin(snSet.getHostname(whichSN),
                                             snSet.getRegistryPort(whichSN));
    }

    /**
     * Deploys an Admin to the specified SN. The type of the Admin is the
     * same as the zone.
     */
    public static CommandServiceAPI deployAdmin(CommandServiceAPI cs,
                                                SNSet snSet,
                                                int whichSN)
        throws Exception {
        return deployAdmin(cs, snSet, whichSN, null);
    }

    /**
     * Deploys an Admin of the specified type to the specified SN. If the type
     * is null the type of the Admin is the same as the zone.
     */
    public static CommandServiceAPI deployAdmin(CommandServiceAPI cs,
                                                SNSet snSet,
                                                int whichSN,
                                                AdminType type)
        throws Exception {

        awaitPlanSuccess(cs,
                         cs.createDeployAdminPlan("deploy admin",
                                                  snSet.getId(whichSN),
                                                  type));
        return StorageNodeUtils.waitForAdmin(snSet.getHostname(whichSN),
                                             snSet.getRegistryPort(whichSN));
    }

    /**
     * Change a parameter for all admins.
     *
     * @param cs the command service API
     * @param paramName the parameter name
     * @param paramVal the parameter value
     */
    public static void changeAllAdminParams(final CommandServiceAPI cs,
                                            final String paramName,
                                            final String paramVal)
        throws Exception {

        final ParameterMap pMap = new ParameterMap();
        pMap.setParameter(paramName, paramVal);
        awaitPlanSuccess(
            cs,
            cs.createChangeAllAdminsPlan(
                "change all admin params " + paramName + "=" + paramVal,
                null, /* null dcid ==> change all admin params */
                pMap));
    }

    /**
     * Make and populate a storage node pool
     * @param cs
     * @param poolName - the pool name
     * @param snIds - the list of storage node ids to add.
     * @throws RemoteException
     */
    public static void makeSNPool(CommandServiceAPI cs,
                                  String poolName,
                                  StorageNodeId... snIds)
        throws RemoteException {

        cs.addStorageNodePool(poolName);
        for (StorageNodeId snId : snIds) {
            cs.addStorageNodeToPool(poolName, snId);
        }
    }

    /**
     * A struct that packages together
     *  - a handle to the CommandService (mimics the CLI)
     *  - a map of datacenter names -> ids (mimics information the system
     *                                      adminstrator should have)
     *  - a SNSet, which mimics the bootstrap information that a system
     *                                      administrator should have)
     */
    public static class SysAdminInfo {

        private final CommandServiceAPI cs;
        private final Map<String,DatacenterId> dcMap;
        private final SNSet snSet;

        SysAdminInfo(CommandServiceAPI cs,
                     SNSet snSet) {
            this.cs = cs;
            this.snSet = snSet;
            this.dcMap = new HashMap<String,DatacenterId>();
        }

        public CommandServiceAPI getCommandService() {
            return cs;
        }

        public DatacenterId getDCId(String datacenterName) {
            return dcMap.get(datacenterName);
        }

        public void putDCId(String datacenterName, DatacenterId dcId) {
            dcMap.put(datacenterName, dcId);
        }

        public void removeDCId(String datacenterName) {
            dcMap.remove(datacenterName);
        }

        public SNSet getSNSet() {
            return snSet;
        }
    }

    /**
     * Manage a set of SNs and keep track of their assigned port ranges and
     * registry ports. This equates to the information that the system
     * administrator of a NoSQL DB deployment records and keeps track of.
     * To use the methods in this class, the calling program should instantiate
     * an instance of SNSet.
     *
     * SNs are kept in a list, and the methods refer to them by their list
     * index.
     */
    public static class SNSet {
        private final static int START_PORT = 13230;
        private final static int HA_RANGE = 5;

        private final String kvstoreName;
        private final int overallStartPort;
        private final List<SNKeeper> keepers;

        /**
         * @param kvstoreName the name of the store
         * @param numSNs the maximum number of SNs that this test program will
         * be using.
         * @param startPort all SNs in the set use a port range starting from
         * this value, because we are assuming this is running on a single
         * machine.
         * @param haRange number of ports used for HA, which therefore governs
         * the number of RNS that can be supported.
         * @param snHosts the array of host names to use for the SNs, or null
         * to use "localhost" for all SNs
         * @param useThreads whether to use threads to run SNs
         * @param snMemoryMB the memory to be associated with SN. This
         * memory is then carved up across RNs, admins etc. hosted by the SN
         * @throws Exception
         */
        SNSet(String kvstoreName, int numSNs, int startPort, int haRange,
              String[] snHosts, boolean useThreads, String snMemoryMB)
            throws Exception {

            this.kvstoreName = kvstoreName;
            this.overallStartPort = startPort;
            if ((snHosts != null) && (numSNs != snHosts.length)) {
                throw new IllegalArgumentException(
                    "snHosts should have length " + numSNs + ", was " +
                    snHosts.length);
            }
            keepers = new ArrayList<SNKeeper>(numSNs);

            for (int i = 0; i < numSNs; i++) {
                final int memoryMB = Integer.parseInt((snMemoryMB != null) ?
                                   snMemoryMB : MB_PER_SN_STRING);
                SNKeeper keeper =
                    new SNKeeper(i,
                                 overallStartPort + (i * 20),
                                 haRange,
                                 (snHosts != null) ? snHosts[i] : "localhost",
                                 (i==0), // createAdmin
                                 useThreads,
                                 memoryMB);
                keepers.add(keeper);
            }
        }

        StorageNodeAgent getSNA(int whichSN) {
            return keepers.get(whichSN).getSNA();
        }

        /**
         * Translate the SN index (the nth SN) into a StorageNodeId.
         * @param whichSN index of the target SN
         * @return StorageNodeId for this nth SN.
         */
        public StorageNodeId getId(int whichSN) {
            return keepers.get(whichSN).getSNId();
        }

        /**
         * Translate StorageNodeId to SN index.
         */
        public int getIndex(StorageNodeId snId) {
            for (int i = 0; i < keepers.size(); i++) {
                if (keepers.get(i).getSNId().equals(snId)) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Translate the SN index (the nth SN) into StorageNodeIds.
         * @param whichSNs list of sn indexes
         * @return a list of StorageNodeIds which correspond to the specified
         * SNs
         */
        public StorageNodeId[] getIds(int... whichSNs) {
            StorageNodeId[] snIds = new StorageNodeId[whichSNs.length];
            int i = 0;
            for (final int whichSN : whichSNs) {
                snIds[i++] = getId(whichSN);
            }
            return snIds;
        }

        public String getHostname(int whichSN) {
            return keepers.get(whichSN).getHostname();
        }

        public int getRegistryPort(int whichSN) {
            return keepers.get(whichSN).getRegistryPort();
        }

        /**
         * Shut down all SNs in the set.
         */
        public void shutdown() {
            for (SNKeeper keeper : keepers) {
                keeper.shutdown();
            }
        }

        /**
         * Restart one SN and wait for all services to come up.
         * @throws Exception
         */
        public void restart(int whichSN, Set<RepNodeId> rns, Logger logger)
            throws Exception {

            SNKeeper keeper = keepers.get(whichSN);
            logger.info("Test is restarting down SN " + keeper.getSNId());
            keeper.restart();
            String hostname = getHostname(whichSN);
            int regPort = getRegistryPort(whichSN);
            if (keeper.hasAdmin()) {
                StorageNodeUtils.waitForAdmin(hostname, regPort);
            }

            for (RepNodeId rnId: rns) {
                StorageNodeUtils.waitForRNAdmin(rnId, keeper.getSNId(),
                                                kvstoreName, hostname,
                                                regPort, 10);
            }
        }

        public MasterBalanceManagerInterface
            getMasterBalanceManager(int whichSN) {
            SNKeeper keeper = keepers.get(whichSN);
            return keeper.getMasterBalanceManager();
        }

        /**
         * Shutdown one SN
         */
        public void shutdown(int whichSN, Logger logger) {
            SNKeeper keeper = keepers.get(whichSN);
            logger.info("Test is shutting down SN " + keeper.getSNId());
            keeper.shutdown();
        }

        /**
         * Deploy SNs in this universe, as determined by the set of list
         * indices provided.  They will all be deployed against the same
         * datacenter.
         * @param cs the CommandService to use.
         * @param dcId Datacenter which will house these SNS.
         * @param whichSNs a list of SN indices. For example, 1,10,13 will
         * deploy the first, tenth and thirteen SN.
         * @throws RemoteException
         */
        public void deploy(CommandServiceAPI cs,
                           DatacenterId dcId,
                        /* TODO: remove if changeParams call below does proves
                         * to be unnecessary.
                         */
                           String snMemoryMB,
                           int... whichSNs)
            throws RemoteException {

            for (int snIndex :  whichSNs) {

                if (keepers.get(snIndex).getSNId() != null) {
                    /* already deployed. */
                    continue;
                }

                keepers.get(snIndex).deploySelf(cs, dcId);
            }

            /* Set the memory for each SN to an AdminUtils default */
            /* TODO: I don't think this is necessary now that SNKeeper
             * has been fixed to deal correctly with memory mb, but
             * retaining for now to minimize changes.
             */
            changeParams(cs, ParameterState.COMMON_MEMORY_MB,
                         snMemoryMB, whichSNs);
        }

        public void changeParams(CommandServiceAPI cs,
                                 String paramName,
                                 String paramVal,
                                 int ... whichSNs)
            throws RemoteException {

            Parameters p = cs.getParameters();
            for (int snIndex : whichSNs) {
                StorageNodeId snId = keepers.get(snIndex).getSNId();
                ParameterMap pMap = p.get(snId).getMap();
                pMap.setParameter(paramName, paramVal);
                awaitPlanSuccess(cs,
                                 cs.createChangeParamsPlan(
                                     "ChangeSNParams_" + snId, snId, pMap));
            }
        }

        /*
         * Change storage dir path and size for specific SN. If this is an add
         * storage dir operation, the target storage dir path will be created
         * by default. If the mount point directory fail to create, an ISE
         * will be thrown to indicate the problem.
         */
        public void changeMountPoint(CommandServiceAPI cs,
                                     boolean add,
                                     String mountPoint,
                                     String mountPointSize,
                                     int whichSN) throws RemoteException {
            changeMountPoint(
                cs, add, mountPoint, mountPointSize, whichSN, true);
        }

        /**
         * Change target SN's storage dir information.
         * @param cs command service API used to change storage dir.
         * @param add whether this is an add or remove storage dir operation.
         * @param mountPoint the path of storage dir to operate on.
         * @param mountPointSize the size of storage dir to operate on.
         * @param whichSN target SN to be changed.
         * @param requireDir whether the storage dir path is required to create
         * before operation.
         */
        void changeMountPoint(CommandServiceAPI cs,
                               boolean add,
                               String mountPoint,
                               String mountPointSize,
                               int whichSN,
                               boolean requireDir)
            throws RemoteException {

            Parameters p = cs.getParameters();

            StorageNodeId snId = keepers.get(whichSN).getSNId();
            if (add && requireDir) {
                File f = new File(mountPoint);
                boolean done = f.mkdir();
                if (!done) {
                    throw new IllegalStateException("Couldn't make directory " +
                                                    mountPoint);
                }
            }

            ParameterMap pMap =  StorageNodeParams.changeStorageDirMap
                (p, p.get(snId), add, mountPoint, mountPointSize);
            awaitPlanSuccess(
                cs,
                cs.createChangeParamsPlan(
                    "ChangeStorageDirectory_" + snId, snId, pMap));
        }

        public void changeRNLogMountPoint(CommandServiceAPI cs, boolean add,
                                          String rnLogMountPoint,
                                          String rnLogMountPointSize,
                                          int whichSN)
                throws RemoteException {

            Parameters p = cs.getParameters();

            StorageNodeId snId = keepers.get(whichSN).getSNId();
            if (add) {
                File f = new File(rnLogMountPoint);
                boolean done = f.mkdir();
                if (!done) {
                    throw new IllegalStateException(
                            "Couldn't make directory " + rnLogMountPoint);
                }
            }

            ParameterMap pMap = StorageNodeParams.changeRNLogDirMap(p,
                    p.get(snId), add, rnLogMountPoint, rnLogMountPointSize);
            awaitPlanSuccess(
                cs,
                cs.createChangeParamsPlan(
                    "ChangeRNLogDirectory_" + snId, snId, pMap));
        }

        public void changeMountPointSize(CommandServiceAPI cs,
                                         String path, String size,
                                         int whichSN)
            throws RemoteException {

            final Parameters p = cs.getParameters();
            final StorageNodeId snId = keepers.get(whichSN).getSNId();
            final ParameterMap pMap =
                  StorageNodeParams.changeStorageDirSize(p, p.get(snId),
                                                         path, size);
            awaitPlanSuccess(
                cs,
                cs.createChangeParamsPlan("ChangeStorageDirectory_" + snId,
                                          snId, pMap));
        }

        public CommandServiceAPI getAdminMaster() {

            /**
             * Retry if a failover is happening.
             */
            for (int i = 0; i < 10; i++ ) {
                for (SNKeeper keeper : keepers) {
                    try {
                        CommandServiceAPI newcs =
                            StorageNodeUtils.waitForAdmin(
                                keeper.getHostname(),
                                keeper.getRegistryPort());
                        if (newcs.getAdminStatus().getReplicationState() ==
                            State.MASTER) {
                            return newcs;
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
            return null;
        }
    }

    /**
     * A struct for keeping a storage node agent and its administration of its
     * available ports in one place.
     */
    private static class SNKeeper {
        private StorageNodeAgent sna;
        private final PortFinder portFinder;

        /*
         * The snId is set once the SN is registered. If null, this is a
         * SN that is awaiting registration.
         */
        private StorageNodeId snId;
        private final int whichSN;
        private final boolean useThreads;
        private final boolean createAdmin;

        SNKeeper(int whichSN, int startPort, int haRange, String hostname,
                 boolean createAdmin, boolean useThreads, int snMemoryMB)
            throws Exception {

            this.whichSN = whichSN;
            this.useThreads = useThreads;
            this.createAdmin = createAdmin;
            portFinder = new PortFinder(startPort, haRange, hostname);
            sna = StorageNodeUtils.createUnregisteredSNA
                (getBootstrapDir(),
                 portFinder,
                 1,    /* capacity */
                 getBootstrapFile(),
                 useThreads,
                 createAdmin,
                 null, 0, 0,
                 (snMemoryMB == 0) ? MB_PER_SN : snMemoryMB);
        }

        boolean hasAdmin() {
            return createAdmin;
        }

        String getHostname() {
            return portFinder.getHostname();
        }

        int getRegistryPort() {
            return portFinder.getRegistryPort();
        }

        StorageNodeId getSNId() {
            return snId;
        }

        private String getBootstrapDir() {
            return TestUtils.getTestDir().toString();
        }

        private String getBootstrapFile() {
            return "config" + whichSN + ".xml";
        }

        /**
         * Deploy the SN.
         */
        void deploySelf(CommandServiceAPI cs, DatacenterId dcid)
            throws RemoteException {

            String hostname = portFinder.getHostname();
            int registryPort = portFinder.getRegistryPort();
            awaitPlanSuccess(cs,
                             cs.createDeploySNPlan(
                                 "deploy storage node", dcid, hostname,
                                 registryPort, null));

            /* Check the resulting topology changes. */
            Topology t = cs.getTopology();
            StorageNode newSN = null;
            for (StorageNode s : t.getStorageNodeMap().getAll()) {
                if (s.getRegistryPort() == registryPort) {
                    newSN = s;
                    break;
                }
            }

            if (newSN == null) {
                throw new IllegalStateException("SN for " + hostname + ":" +
                                                registryPort +
                                                " wasn't created");
            }
            snId = newSN.getResourceId();
        }

        void restart()
            throws Exception {

            assert sna == null;

            sna = StorageNodeUtils.startSNA(getBootstrapDir(),
                                            getBootstrapFile(),
                                            useThreads,
                                            false);
        }

        MasterBalanceManagerInterface getMasterBalanceManager() {
            return sna.getMasterBalanceManager();
        }

        void shutdown() {
            if (sna != null) {
                sna.shutdown(true /* stopServices */, false /*force*/);
                sna = null;
            }
        }

        StorageNodeAgent getSNA() {
            return sna;
        }
    }

    /**
     * Check for violations and wait until the match the specified counts and
     * violation classes.
     */
    public static void verifyViolations(CommandServiceAPI cs,
                                        Object... countsAndProblemClasses)
        throws RemoteException {

        verifyProblems(cs, true /* violations */, countsAndProblemClasses);
    }

    /**
     * Check for warnings and wait until the match the specified counts and
     * warning classes.
     */
    public static void verifyWarnings(CommandServiceAPI cs,
                                      Object... countsAndProblemClasses)
        throws RemoteException {

        verifyProblems(cs, false /* violations */, countsAndProblemClasses);
    }

    private static void verifyProblems(CommandServiceAPI cs,
                                       boolean violations,
                                       Object... countsAndProblemClasses)
        throws RemoteException {

        if (countsAndProblemClasses.length % 2 != 0) {
            throw new IllegalArgumentException(
                "The countsAndProblemClasses argument must have an even" +
                " number of elements");
        }
        final Map<Class<?>, Integer> expectedProblems = new HashMap<>();
        for (int i = 0; i < countsAndProblemClasses.length; i += 2) {
            expectedProblems.put((Class<?>) countsAndProblemClasses[i+1],
                                 (Integer) countsAndProblemClasses[i]);
        }
        final AtomicReference<Map<Class<?>, Integer>> actualProblems =
            new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final boolean result = new PollCondition(500, 60000) {
            @Override
            protected boolean condition() {
                actualProblems.set(null);
                exception.set(null);
                try {
                    actualProblems.set(
                        collectProblems(
                            violations,
                            cs.verifyConfiguration(false, false, false)));
                    return expectedProblems.equals(actualProblems.get());
                } catch (Exception e) {
                    exception.set(e);
                    return false;
                }
            }
        }.await();
        if (!result) {
            final VerifyResults results =
                cs.verifyConfiguration(false, false, false);
            assertEquals("Unexpected " +
                         (violations ? "violations: " : "warnings: ") +
                         (violations ?
                          results.getViolations() :
                          results.getWarnings()) +
                         ((exception.get() != null) ?
                          " exception: " + exception.get() :
                          ""),
                         expectedProblems, actualProblems.get());
        }
    }

    private static final Map<Class<?>, Integer> collectProblems(
        boolean violations, VerifyResults results) {

        final Map<Class<?>, Integer> foundProblems = new HashMap<>();
        List<Problem> problems =
            violations ? results.getViolations() : results.getWarnings();
        for (final Problem problem : problems) {
            final Integer count = foundProblems.get(problem.getClass());
            foundProblems.put(problem.getClass(),
                              (count == null ? 1 : count + 1));
        }
        return foundProblems;
    }
}
