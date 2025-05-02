/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.admin.plan;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.task.StartAdminV2;
import oracle.kv.impl.admin.plan.task.StopAdmin;
import oracle.kv.impl.admin.plan.task.VerifySNStorageDirSizes;
import oracle.kv.impl.admin.plan.task.WaitForAdminState;
import oracle.kv.impl.admin.plan.task.WriteNewANParams;
import oracle.kv.impl.admin.plan.task.WriteNewAdminParams;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.admin.plan.task.WriteNewSNParams;
import oracle.kv.impl.mgmt.MgmtUtil;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

public class ChangeSNParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    protected ParameterMap newParams;

    /*
     * Note that in an upgrade situation -- plan created with previous version,
     * but run with this version -- this field will be null, which should just
     * mean that the preExecuteCheck will not be performed. That seems harmless,
     * but making sure that an upgrade behaves reasonably is part of
     * what we need to do when adding fields to a plan.
     */
    private ParameterMap oldStorageDirMap;
    protected StorageNodeId storageNodeId;

    public ChangeSNParamsPlan(String name,
                              Planner planner,
                              StorageNodeId snId,
                              ParameterMap newParams) {

        super(name, planner);

        this.newParams = newParams;
        this.storageNodeId = snId;

        /**
         * Set correct storage node id because this is going to be stored.
         */
        newParams.setParameter(ParameterState.COMMON_SN_ID,
                               Integer.toString(snId.getStorageNodeId()));

        validateParams(planner, snId);

        /*
         * Check for validation of storage directory sizes of the SN as a task
         * at the time of execution of the plan.
         * It will not proceed with the other tasks of the plan if
         * VerifySNStorageDirSizes task fails.
         */

        if (newParams.getName().equals(ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
            Admin admin = planner.getAdmin();
            oldStorageDirMap = admin.getStorageNodeParams(snId).getStorageDirMap();
            addTask(new VerifySNStorageDirSizes(this, newParams));
        }

        addTask(new WriteNewSNParams(this, snId, newParams, true));

        /*
         * If we have changed the capacity, file system percentage, memory
         * setting, numCPUS or dns cache ttl values of this SN, we may have to
         * change the params for any RNs on this SN.  Also check for storage
         * directory size changes.
         */
        if (newParams.exists(ParameterState.COMMON_MEMORY_MB) ||
            newParams.exists(ParameterState.SN_RN_HEAP_PERCENT) ||
            newParams.exists(ParameterState.JVM_OVERHEAD_PERCENT) ||
            newParams.exists(ParameterState.COMMON_CAPACITY) ||
            newParams.exists(ParameterState.COMMON_NUMCPUS) ||
            newParams.exists(ParameterState.SN_ROOT_DIR_SIZE) ||
            newParams.exists(ParameterState.COMMON_DNS_CACHE_TTL) ||
            newParams.exists(ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL)) {
            updateRNParams(snId, newParams);
        } else if (newParams.getName().
                   equals(ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
            updateRNStorageDirs(snId, newParams);
            /*
             * TODO : Yet to add support for changing
             * BOOTSTRAP_ADMIN_MOUNT POINTS and BOOTSTRAP_RNLOG_MOUNT_POINTS
             */
        }

        if (newParams.exists(ParameterState.COMMON_DNS_CACHE_TTL) ||
            newParams.exists(ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL)) {
            updateANParams(snId, newParams);
            updateAdminParams(snId, newParams);
        }

        /*
         * This is a no-restart plan at this time, we are done.
         */
    }

    /**
     * It is a check prior to the plan execution. It checks if the storage
     * directory map has been changed since plan creation during plan
     * change-storagedir command usage. If check fails, it will stop
     * the execution of further task(s) in the plan throwing proper error
     * message as described. The plan fails and the new parameters of the SN
     * will not be updated. It will be same as before the execution of the plan.
     */

    @Override
    public void preExecuteCheck(boolean force, Logger plannerlogger) {
        if(oldStorageDirMap != null) {
            Admin admin = planner.getAdmin();
            ParameterMap currentMap = admin.getStorageNodeParams(storageNodeId).
                getStorageDirMap();
            ParameterMap newStorageDirMap = newParams.copy();
            newStorageDirMap.remove("storageNodeId");
            if (currentMap!=null && !currentMap.equals(oldStorageDirMap)) {
                String msg = String.format(
                    "The plan cannot be run because the storage directory " +
                    "map of " + storageNodeId + " has changed since plan " +
                    "creation.\n" +
                    "New storage dir map :\n%s\n" +
                    "Storage dir map at the time of plan creation :\n%s\n" +
                    "Current storage dir map :\n%s",
                    newStorageDirMap, oldStorageDirMap, currentMap);
                throw new IllegalCommandException(msg);
            }
        }
    }

    @Override
    public String getDefaultName() {
        return "Change Storage Node Params";
    }

    private void validateParams(Planner p, StorageNodeId snId) {
        if (newParams.getName().
            equals(ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
            Admin admin = p.getAdmin();
            Parameters parameters = admin.getCurrentParameters();
            /*
             * Will throw an IllegalCommandException if a storage directory
             * is in use.
             */
            StorageNodeParams.validateStorageDirMap(newParams, parameters,snId);
        }
        if (newParams.getName().equals(ParameterState.SNA_TYPE)) {
            String error = validateMgmtParams(newParams);
            if (error != null) {
                throw new IllegalCommandException(error);
            }
            /* Let the StorageNodeParams class validate */
            new StorageNodeParams(newParams).validate();
        }
    }

    /**
     * Return a non-null error message if incorrect mgmt param values are
     * present.
     */
    public static String validateMgmtParams(ParameterMap aParams) {
        if (!aParams.exists(ParameterState.COMMON_MGMT_CLASS)) {
            return null;
        }
        Parameter mgmtClass
            = aParams.get(ParameterState.COMMON_MGMT_CLASS);
        if (! MgmtUtil.verifyImplClassName(mgmtClass.asString())) {
            return
                ("The given value " + mgmtClass.asString() +
                 " is not allowed for the parameter " +
                 mgmtClass.getName());
        }
        return null;
    }

    public StorageNodeParams getNewParams() {
        return new StorageNodeParams(newParams);
    }

    /**
     * Generate tasks to update the JE cache size, JVM args or DNS cache TTL
     * values for any RNS on this SN.
     */
    private void updateRNParams(StorageNodeId snId,
                                ParameterMap newMap) {

        Admin admin = planner.getAdmin();
        StorageNodeParams snp = admin.getStorageNodeParams(snId);
        ParameterMap policyMap = admin.getCurrentParameters().copyPolicies();

        /* Find the capacity value to use */
        int capacity = snp.getCapacity();
        if (newMap.exists(ParameterState.COMMON_CAPACITY)) {
            capacity = newMap.get(ParameterState.COMMON_CAPACITY).asInt();
        }

        /*
         * Find the number of RNs hosted on this SN; that affects whether we
         * modify the heap value.
         */
        final int numHostedRNs =
            admin.getCurrentTopology().getHostedRepNodeIds(snId).size();

        final int numHostedANs =
                admin.getCurrentTopology().getHostedArbNodeIds(snId).size();

        /* Find the RN heap memory percent to use */
        int rnHeapPercent = snp.getRNHeapPercent();
        if (newMap.exists(ParameterState.SN_RN_HEAP_PERCENT)) {
            rnHeapPercent =
                newMap.get(ParameterState.SN_RN_HEAP_PERCENT).asInt();
        }

        /* Find the memory mb value to use */
        int memoryMB = snp.getMemoryMB();
        if (newMap.exists(ParameterState.COMMON_MEMORY_MB)) {
            memoryMB = newMap.get(ParameterState.COMMON_MEMORY_MB).asInt();
        }

        /* Find the numCPUs value to use */
        int numCPUs = snp.getNumCPUs();
        if (newMap.exists(ParameterState.COMMON_NUMCPUS)) {
            numCPUs = newMap.get(ParameterState.COMMON_NUMCPUS).asInt();
        }

        Parameter newRootSize = snp.getMap().
                                    get(ParameterState.SN_ROOT_DIR_SIZE);
        if (newParams.exists(ParameterState.SN_ROOT_DIR_SIZE)) {
            newRootSize = newMap.get(ParameterState.SN_ROOT_DIR_SIZE);
        }

        /* Find the -XX:ParallelGCThread flag to use */
        int gcThreads = StorageNodeParams.calcGCThreads
            (numCPUs, capacity, snp.getGCThreadFloor(),
             snp.getGCThreadThreshold(), snp.getGCThreadPercent());

        HashMap<RepNodeId, ParameterMap> pmap =
            new HashMap<RepNodeId, ParameterMap>();
        for (RepNodeParams rnp :
             admin.getCurrentParameters().getRepNodeParams()) {
            if (!rnp.getStorageNodeId().equals(snId)) {
                continue;
            }

            RNHeapAndCacheSize heapAndCache =
                StorageNodeParams.calculateRNHeapAndCache
                (policyMap, capacity, numHostedRNs, memoryMB, rnHeapPercent,
                 rnp.getRNCachePercent(), numHostedANs);
            ParameterMap rnMap = new ParameterMap(ParameterState.REPNODE_TYPE,
                                                  ParameterState.REPNODE_TYPE);

            /*
             * Hang onto the current JVM params in a local variable. We may
             * be making multiple changes to them, if we change both heap and
             * parallel gc threads.
             */
            String currentJavaMisc = rnp.getJavaMiscParams();
            if (rnp.getMaxHeapMB() != heapAndCache.getHeapMB()) {
                /* Set both the -Xms and -Xmx flags */
                currentJavaMisc = rnp.replaceOrRemoveJVMArg
                    (currentJavaMisc, RepNodeParams.XMS_FLAG,
                     heapAndCache.getHeapValAndUnit());
                currentJavaMisc = rnp.replaceOrRemoveJVMArg
                    (currentJavaMisc, RepNodeParams.XMX_FLAG,
                     heapAndCache.getHeapValAndUnit());
                rnMap.setParameter(ParameterState.JVM_MISC, currentJavaMisc);
            }

            if (rnp.getRNCachePercent() != heapAndCache.getCachePercent()) {
                rnMap.setParameter(ParameterState.RN_CACHE_PERCENT,
                                Long.toString (heapAndCache.getCachePercent()));
            }

            if (rnp.getJECacheSize() != heapAndCache.getCacheBytes()) {
                rnMap.setParameter(ParameterState.JE_CACHE_SIZE,
                                   Long.toString(heapAndCache.getCacheBytes()));
            }

            if (gcThreads != 0) {
                /* change only if old and new values don't match */
                String oldGc = RepNodeParams.parseJVMArgsForPrefix
                    (RepNodeParams.PARALLEL_GC_FLAG, currentJavaMisc);
                if (oldGc != null) {
                    if (Integer.parseInt(oldGc) != gcThreads) {
                        currentJavaMisc =
                            rnp.replaceOrRemoveJVMArg(currentJavaMisc,
                                              RepNodeParams.PARALLEL_GC_FLAG,
                                              Integer.toString(gcThreads));
                        /* Set the concurrent threads the same as parallel 
                         * threads */
                        currentJavaMisc =
                            rnp.replaceOrRemoveJVMArg(currentJavaMisc,
                                              RepNodeParams.CONCURRENT_GC_FLAG,
                                              Integer.toString(gcThreads));
                        rnMap.setParameter
                            (ParameterState.JVM_MISC, currentJavaMisc);
                    }
                }
            }

            /* If the RN mount point is null, it is on the root */
            if ((rnp.getStorageDirectoryPath() == null) && (newRootSize != null)) {
                rnMap.setParameter(ParameterState.RN_MOUNT_POINT_SIZE,
                                   newRootSize.asString());
            }

            /* Sets the dns cache ttl values if necessary */
            rnMap.duplicateCacheTTLValues(newMap);

            if (!rnMap.isEmpty()) {
                pmap.put(rnp.getRepNodeId(), rnMap);
            }

        }

        generateRNUpdateTasksV2(pmap, snId);
    }

    /**
     * Generate tasks to update storage directory sizes for RNs on the specified
     * SN. An update is done if the RN has a defined storage directory and the
     * size has changed.
     */
    private void updateRNStorageDirs(StorageNodeId snId, ParameterMap newMap) {
        final Admin admin = planner.getAdmin();
        final HashMap<RepNodeId, ParameterMap> pmap =
            new HashMap<RepNodeId, ParameterMap>();
        for (RepNodeParams rnp :
                admin.getCurrentParameters().getRepNodeParams()) {
            if (!rnp.getStorageNodeId().equals(snId)) {
                continue;
            }

            /* If the current storage dir is null, this RN is in the root dir */
            final String rnPath = rnp.getStorageDirectoryPath();
            if (rnPath == null) {
                continue;
            }

            /*
             * If the RN's storage dir is not in the parameters, something is
             * wrong.
             */
            final Parameter updatedSD = newMap.get(rnPath);
            if (updatedSD == null) {
                throw new IllegalCommandException(
                             "The storage directory for " + rnp.getRepNodeId() +
                             " is not defined in the parameters for " + snId);
            }

            /* Update the RN only if the size has changed */
            final long updatedSize = SizeParameter.getSize(updatedSD);
            if (updatedSize == rnp.getStorageDirectorySize()) {
                continue;
            }

            ParameterMap rnMap = new ParameterMap(ParameterState.REPNODE_TYPE,
                                                  ParameterState.REPNODE_TYPE);
            rnMap.setParameter(ParameterState.RN_MOUNT_POINT_SIZE,
                               updatedSD.asString());
            pmap.put(rnp.getRepNodeId(), rnMap);
        }

        generateRNUpdateTasksV2(pmap, snId);
    }

    /**
     * Generates tasks to update the RN parameters, and restart the RN
     * if needed.
     */
    private void generateRNUpdateTasksV2(Map<RepNodeId, ParameterMap> pmap,
                                         StorageNodeId snId) {

        for (Entry<RepNodeId, ParameterMap>  pval : pmap.entrySet()) {
            addTask(new WriteNewParams(this,
                                       pval.getValue(),
                                       pval.getKey(),
                                       snId,
                                       true));
        }
    }

    /**
     * Generate tasks to update the DNS cache TTL values for any ANs on this
     * SN.
     */
    private void updateANParams(StorageNodeId snId,
                                ParameterMap newMap) {

        final Admin admin = planner.getAdmin();

        final Map<ArbNodeId, ParameterMap> pmap = new HashMap<>();
        for (ArbNodeParams anp :
             admin.getCurrentParameters().getArbNodeParams()) {
            if (!anp.getStorageNodeId().equals(snId)) {
                continue;
            }
            final ParameterMap anMap =
                new ParameterMap(ParameterState.ARBNODE_TYPE,
                                 ParameterState.ARBNODE_TYPE);
            anMap.duplicateCacheTTLValues(newMap);
            if (!anMap.isEmpty()) {
                pmap.put(anp.getArbNodeId(), anMap);
            }
        }

        generateANUpdateTasksV2(pmap, snId);
    }

    /**
     * Generates tasks to update the AN parameters, and restart the AN
     * if needed.
     */
    private void generateANUpdateTasksV2(Map<ArbNodeId, ParameterMap> pmap,
                                         StorageNodeId snId) {

       for (Entry<ArbNodeId, ParameterMap>  pval : pmap.entrySet()) {
            addTask(new WriteNewANParams(
                            this, pval.getValue(), pval.getKey(), snId, true));
        }
    }

    /**
     * Generate tasks to update the DNS cache TTL values for any Admins on this
     * SN.
     */
    private void updateAdminParams(StorageNodeId snId,
                                   ParameterMap newMap) {
        final Admin admin = planner.getAdmin();

        final Map<AdminId, ParameterMap> pmap = new HashMap<>();
        for (AdminParams ap :
             admin.getCurrentParameters().getAdminParams()) {
            if (!ap.getStorageNodeId().equals(snId)) {
                continue;
            }
            final ParameterMap adminMap =
                new ParameterMap(ParameterState.ADMIN_TYPE,
                                 ParameterState.ADMIN_TYPE);
            adminMap.duplicateCacheTTLValues(newMap);
            if (!adminMap.isEmpty()) {
                /*
                * Make sure the new values do not equal to the current ones
                * since we do not want to restart Admin when we are not
                * changing the cache ttl parameters. The WriteNewAdminParams
                * task also makes this check, but unfortunately the task
                * currently do not include start and stop admin routine, and
                * hence we need to check here as well. A better solution would
                * be to include start and stop Admin inside the
                * WriteNewAdminParams task.
                *
                * TODO: remove this when the WriteNewAdminParams includes the
                * start and stop admin routines as with the WriteNewParams
                * task.
                */
                final Parameter newPositiveTTL = adminMap.get(
                    ParameterState.COMMON_DNS_CACHE_TTL_VALUE);
                final Parameter existingPositiveTTL = ap.getMap().get(
                    ParameterState.COMMON_DNS_CACHE_TTL_VALUE);
                final Parameter newNegativeTTL = adminMap.get(
                    ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE);
                final Parameter existingNegativeTTL = ap.getMap().get(
                    ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE);
                if (!newPositiveTTL.equals(existingPositiveTTL) ||
                    !newNegativeTTL.equals(existingNegativeTTL)) {
                    pmap.put(ap.getAdminId(), adminMap);
                }
            }
        }
        generateAdminUpdateTasks(pmap, snId);
    }

    private void generateAdminUpdateTasks(Map<AdminId, ParameterMap> pmap,
                                          StorageNodeId snId) {
        for (Entry<AdminId, ParameterMap>  pval : pmap.entrySet()) {
            AdminId adminId = pval.getKey();
            addTask(new WriteNewAdminParams(
                            this, pval.getValue(), adminId, snId));
            addTask(new StopAdmin(this, snId, adminId, false));
            addTask(new StartAdminV2(this, snId, adminId, false));
            addTask(new WaitForAdminState(
                            this, snId, adminId, ServiceStatus.RUNNING));
        }
    }

    @Override
    public void stripForDisplay() {
        newParams = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
