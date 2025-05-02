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

package oracle.kv.impl.admin.topo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.topo.Validations.ANNotAllowedOnSN;
import oracle.kv.impl.admin.topo.Validations.ANProximity;
import oracle.kv.impl.admin.topo.Validations.ANWrongDC;
import oracle.kv.impl.admin.topo.Validations.BadAdmin;
import oracle.kv.impl.admin.topo.Validations.EmptyZone;
import oracle.kv.impl.admin.topo.Validations.ExcessANs;
import oracle.kv.impl.admin.topo.Validations.ExcessAdmins;
import oracle.kv.impl.admin.topo.Validations.ExcessRNs;
import oracle.kv.impl.admin.topo.Validations.HelperParameters;
import oracle.kv.impl.admin.topo.Validations.InsufficientANs;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MissingAdminDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MissingStorageDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.NoPartition;
import oracle.kv.impl.admin.topo.Validations.NoPrimaryDC;
import oracle.kv.impl.admin.topo.Validations.NonOptimalNumPartitions;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.RNHeapExceedsSNMemory;
import oracle.kv.impl.admin.topo.Validations.RNProximity;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.admin.topo.Validations.StorageDirectorySizeImbalance;
import oracle.kv.impl.admin.topo.Validations.StorageNodeMissing;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.admin.topo.Validations.UnevenANDistribution;
import oracle.kv.impl.admin.topo.Validations.WrongAdminType;
import oracle.kv.impl.admin.topo.Validations.WrongNodeType;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.rep.NodeType;

/**
 * Topologies are governed by constraints on the relationships of components
 * its use of physical resources.
 *
 * Component relationship constraints:
 * ----------------------------------
 * Each datacenter must have a complete copy of the data.
 * A shard should be tolerant of a single point of failure, and to facilitate
 *  that, each RN of a shard must be on a different SN
 * The number of partitions is fixed for the lifetime of a store.
 * Each shard must have an RN in an SN in a primary data center
 *
 * Physical resource constraints:
 * ------------------------------
 * Each SN can only host a maximum of <capacity> number of RNs
 * Additional rules will be added about network affinity, physical fault groups,
 * etc
 *
 * Inputs to a topology:
 * DC   Data center
 * NS   Network switch      Optional
 * SN   Storage node        Associated with a DC, possibly a NS
 * RF   Replication factor  Per data center, associated with S
 * P    Partition           Administrator   # fixed for life of store
 * C    Capacity            Administrator   Per SN
 * SD   Storage directory   Administrator   Optional, per SN
 * RD   RN log directory    Administrator   Optional, per SN
 * ADM  Admin service       Administrator
 *
 * Outputs (calculated by topology builder)
 * S    Shard (formally replication group)
 * RN   Replication node Associated with a S, assigned to a SN
 * ARB  Arbiter          Associated with a S, assigned to a SN
 *
 * The following rules are used to enforce the constraints:
 *
 *  1. Each datacenter must have the same set of shards.
 *  2. Each shard must have repFactor number of RNs
 *  3. Each RN and arbiter of an S must reside on a different SN.
 *  3a. If, and only if, the DC RF is 2, an ARB must be deployed for each S (on
 *      an SN different from the SNs hosting the shard's RNs).
 *  4. The #RNSN {@literal <=} CSN
 *  5. Each shard must have an RN in an SN in a primary datacenter
 *  6. There should be no empty zones.
 *  7. Arbiters will be hosted only when the Primary RF = 2 and when there is
 *     a primary datacenter that allow arbiters.
 *  8. Arbiters are hosted in a single primary datacenter that allows arbiters.
 *  9. Arbiters are hosted in a primary datacenter that is configured to host
 *     arbiter and the RF is zero if this type of datacenter exists.
 *  10. Arbiters will be hosted only in SNs that are configured to allow
 *      hosting Arbiters.
 *  11. Each shard must have one arbiter when the Primary RF = 2 and there is
 *      a primary datacenter that allows arbiters.
 *  12. The arbiter hosting primary datacenter must have at least RF + 1 number
 *      of SNs (excluding capacity = 0 SNs that dont allow arbiters) and at
 *      least one SN that can host arbiters.
 *
 * Warnings:
 *
 * A SN is under capacity.
 * A store should have a additional shards.
 * A shard has no partitions.
 *
 *
 * Not yet implemented:
 *  If NS are present, then depending on the network affinity mode2,
 *        1. each RN of an S must reside on the same NS or
 *        2. each RN of an S must reside on different NSs.
 *
 * The decision to deploy an ARB is per-DC; therefore each S could have an ARB
 * in each DC.
 *
 * There are two network affinity modes, a) optimize for performance, requiring
 * that replication nodes of the same shard reside on the same switch, and b)
 * optimize for availability, requiring that replication nodes of the same
 * shard reside on different switches.
 */
public class Rules {
    private static final int MAX_REPLICATION_FACTOR = 20;

    /**
     * Validation checks that apply to a single topology are:
     * -Every datacenter that is present in the pool of SNs used to deploy the
     *   topology has a complete copy of the data and holds RF number of Admins
     * -all shards house at least one partition
     * -all shards have the datacenter-specified repfactor in each datacenter
     * -all shards have their RNs on different nodes.
     * -each SN is within capacity.
     */
    public static Results validate(Topology topo,
                                   Parameters params,
                                   boolean topoIsDeployed) {

        Results results = new Results();

        /*
         * Do some processing of the topology and parameters to map out
         * relationships that are embodied in the topo and params, but are
         * not stated in a convenient way for validation.
         */

        /*
         * Hang onto which shards are in which datacenters, and how many
         * RNs are in each shard (by datacenter).
         *
         * Also sort RNs by SN, for memory size validation.
         */
        Map<DatacenterId, Map<RepGroupId, Integer>> shardsByDC =
            new HashMap<>();
        Map<StorageNodeId, Set<RepNodeId>> rnsPerSN = new HashMap<>();
        preProcessTopology(topo, shardsByDC, rnsPerSN);

        /* Do all DCs have the same set of shards? */
        checkDcsShardSets(results, shardsByDC, topo);

        /* Do all shards for a DC have the right repfactor number of RNs? */
        checkShardsRepfactor(results, shardsByDC, topo);

        /* All RNs of a single shard must be on different SNs. */
        checkShardRNsStatus(results, topo);

        /* Each shard must have an RN in an SN in a primary DC */
        checkShardRNSNPrimaryDC(results, shardsByDC, topo);

        /* Are all SNs still available in the current store? */
        checkSNsAvailable(results, topo, params);

        // TODO - this check is suspect since parameters may not be set - move
        // to topoIsDeployed clause?
        /* Are all SNs are within capacity?*/
        checkSNCapacity(results, topo, params);

        /* Check the storage directories */
        checkStorageDirectories(results, topo, params);

        if (topoIsDeployed) {

            /* Check RN node type versus zone type */
            checkRepNodeTypes(topo, params, results);

            /* Check the Admins */
            checkAdmins(topo, params, results);

            /*
             * Check environment placement.
             */
            findMultipleRNsInRootDir(topo, params, results);

            /*
             * SN memoryMB and RN heap are optional parameters. If these have
             * been specified, make sure that the sum of all RN JVM heaps does
             * not exceed the memory specified on the SN.
             */
            checkMemoryUsageOnSN(rnsPerSN, params, results);

            checkPartitionLayout(topo, params, results);

            /* Check topology helper host parameters */
            checkTopoHelperRules(topo, results, params);
        }
        /* Check for empty zone.*/
        checkEmptyZone(topo, results);

        /* Check all the arbiter rules */
        checkArbiterRules(topo, results, params);

        return results;
    }

    /**
     * Populate the shardsByDC and rnsPerSN maps.
     *
     * ShardsByDC maps each datacenter ID to a map from shard ID to RN count.
     * If the data center has a replication factor of 0 and no RNs for a given
     * shard, then the shard ID to RN count map will be empty.
     *
     * RNsPerSN maps storage node ID to a set of the RN IDs of the RNs on that
     * SN. If the SN has no RNs, then the SN ID will have no entry.
     */
    private static void preProcessTopology(Topology topo,
                    Map<DatacenterId, Map<RepGroupId, Integer>> shardsByDC,
                    Map<StorageNodeId, Set<RepNodeId>> rnsPerSN) {

        for (Datacenter dc : topo.getSortedDatacenters()) {
            shardsByDC.put(dc.getResourceId(),
                           new HashMap<RepGroupId, Integer>());
        }

        for (RepNode rn : topo.getSortedRepNodes()) {
            StorageNodeId snId = rn.getStorageNodeId();
            StorageNode hostSN = topo.get(snId);
            DatacenterId dcId = hostSN.getDatacenterId();
            RepGroupId rgId = rn.getRepGroupId();

            Map<RepGroupId, Integer> shardCount = shardsByDC.get(dcId);
            Integer count = shardCount.get(rgId);
            if (count == null) {
                shardCount.put(rgId, 1);
            } else {
                shardCount.put(rgId, count + 1);
            }

            Set<RepNodeId> rnIds = rnsPerSN.get(snId);
            if (rnIds == null) {
                rnIds = new HashSet<>();
                rnsPerSN.put(snId, rnIds);
            }
            rnIds.add(rn.getResourceId());
        }
    }

    private static void checkDcsShardSets(Results results,
                    Map<DatacenterId, Map<RepGroupId, Integer>> shardsByDC,
                    Topology topo) {

        Set<RepGroupId> allShardIds = topo.getRepGroupIds();
        for (Map.Entry<DatacenterId, Map<RepGroupId, Integer>> dcInfo :
                 shardsByDC.entrySet()) {

            final DatacenterId dcId = dcInfo.getKey();
            final int repFactor = topo.get(dcId).getRepFactor();

            /* No shards for RF=0 */
            if (repFactor == 0) {
                continue;
            }

            /*
             * Find the shards that are in the topology, but are not in this
             * datacenter.
             */
            Set<RepGroupId> shards = dcInfo.getValue().keySet();
            Set<RepGroupId> missing = relComplement(allShardIds, shards);
            for (RepGroupId rgId : missing) {
                results.add(new InsufficientRNs(dcId, repFactor,
                                                rgId, repFactor));
            }
        }
    }

    private static void checkShardsRepfactor(Results results,
                    Map<DatacenterId, Map<RepGroupId, Integer>> shardsByDC,
                    Topology topo) {

        for (Map.Entry<DatacenterId, Map<RepGroupId, Integer>> dcInfo :
                 shardsByDC.entrySet()) {
            /* RepFactor is set per datacenter */
            DatacenterId dcId = dcInfo.getKey();
            int repFactor = topo.get(dcId).getRepFactor();

            for (Map.Entry<RepGroupId, Integer> shardInfo :
                     dcInfo.getValue().entrySet()) {
                int numRNs = shardInfo.getValue();
                if (numRNs < repFactor) {
                    results.add(new InsufficientRNs(dcId,
                                                    repFactor,
                                                    shardInfo.getKey(),
                                                    repFactor - numRNs));
                } else if (numRNs > repFactor) {
                    results.add(new ExcessRNs(dcId,
                                              repFactor,
                                              shardInfo.getKey(),
                                              numRNs - repFactor));
                }
            }
        }
    }

    private static void checkShardRNsStatus(Results results, Topology topo) {

        for (RepGroupId rgId : topo.getRepGroupIds()) {
            /* Organize RNIds by SNId */
            Map<StorageNodeId, Set<RepNodeId>> rnIdBySNId = new HashMap<>();

            for (RepNode rn : topo.get(rgId).getRepNodes()) {
                StorageNodeId snId = rn.getStorageNodeId();
                Set<RepNodeId> rnIds = rnIdBySNId.get(snId);
                if (rnIds == null) {
                    rnIds = new HashSet<>();
                    rnIdBySNId.put(rn.getStorageNodeId(), rnIds);
                }
                rnIds.add(rn.getResourceId());
            }

            /*
             * If any of the SNs used by this shard have more than one RN,
             * report it.
             */
             for (Map.Entry<StorageNodeId, Set<RepNodeId>> entry :
                rnIdBySNId.entrySet()) {
                if (entry.getValue().size() > 1) {
                    StorageNodeId snId = entry.getKey();
                    results.add(new RNProximity(snId, rgId,
                                           new ArrayList<>(entry.getValue())));
                }
             }
        }
    }

    private static void checkShardRNSNPrimaryDC(Results results,
                    Map<DatacenterId, Map<RepGroupId, Integer>> shardsByDC,
                    Topology topo) {

        final Set<RepGroupId> shardsWithoutPrimaryDC = topo.getRepGroupIds();
        for (final Map.Entry<DatacenterId, Map<RepGroupId, Integer>> dcInfo :
                 shardsByDC.entrySet()) {
            final DatacenterId dcId = dcInfo.getKey();
            final Datacenter dc = topo.get(dcId);
            if (dc.getDatacenterType().isPrimary()) {
                final Map<RepGroupId, Integer> repGroupInfo =
                    dcInfo.getValue();
                shardsWithoutPrimaryDC.removeAll(repGroupInfo.keySet());
            }
        }
        for (final RepGroupId rgId : shardsWithoutPrimaryDC) {
            results.add(new NoPrimaryDC(rgId));
        }
    }

    private static void checkSNsAvailable(Results results,
                                          Topology topo,
                                          Parameters params) {

        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            final StorageNodeParams snParams = params.get(snId);

            /*
             * snParams may be null if the Storage Node(s) does not exist
             * in the current store. Might be removed by the user earlier.
             */
            if (snParams == null) {
                results.add(new StorageNodeMissing(snId));
            }
        }
    }

    private static void checkSNCapacity(Results results,
                                        Topology topo,
                                        Parameters params) {

        CapacityProfile profile = getCapacityProfile(topo, params);
        for (RulesProblem p : profile.getProblems()) {
            results.add(p);
        }

        /* Warn about under capacity SNs. */
        for (RulesProblem w : profile.getWarnings()) {
            results.add(w);
        }
    }

    private static void checkStorageDirectories(Results results,
                    Topology topo, Parameters params) {

        /*
         * Check the storage directories for RN env, RN log
         * and Admin env
         */
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            final StorageNodeParams snp = params.get(snId);

            /*
             * snp may be null if the Storage Node(s) does not exist
             * in the current store. Might be removed by the user earlier.
             */
            if (snp == null) {
                continue;
            }

            final ParameterMap sdMap = snp.getStorageDirMap();
            if(sdMap != null && !sdMap.isEmpty()) {

                /* No storage size specified for storage directories */
                final List<String> paths = new ArrayList<>();
                for (Parameter p : sdMap) {
                    if (SizeParameter.getSize(p) <= 0) {
                        paths.add(p.getName());
                    }
                }
                if (!paths.isEmpty()) {
                    results.add(new MissingStorageDirectorySize(snId,
                                                     snp.getStorageDirPaths()));
                }
            } else {
                String rootDirPath = snp.getRootDirPath();
                long rootDirSize = snp.getRootDirSize();
                if (rootDirSize <= 0) {
                    results.add(new MissingRootDirectorySize(snId,
                                                             rootDirPath));
                }
            }

            /*
             * TODO : We will need to enable MissingRNlogDirectorySize
             * check since currently we may not be exposing RN log dir size
             * initially. Currently just added support.
             */
            final ParameterMap rnLogMap = snp.getRNLogDirMap();
            if(rnLogMap != null && !rnLogMap.isEmpty()) {

                /* No RN log size specified for RN log directories */
                final List<String> paths = new ArrayList<>();
                for (Parameter p : rnLogMap) {
                    if (SizeParameter.getSize(p) <= 0) {
                        paths.add(p.getName());
                    }
                }
                if (!paths.isEmpty()) {
                    /*results.add(new MissingRNLogDirectorySize(snId,
                                                   snp.getRNLogDirPaths()));*/
                }
            }

            final ParameterMap adminDirMap = snp.getAdminDirMap();
            if(adminDirMap != null && !adminDirMap.isEmpty()) {

                /* No admin dirsize specified for admin directory */
                final List<String> paths = new ArrayList<>();
                for (Parameter p : adminDirMap) {
                    if (SizeParameter.getSize(p) <= 0) {
                        paths.add(p.getName());
                    }
                }
                if (!paths.isEmpty()) {
                    results.add(new MissingAdminDirectorySize(snId,
                                                   snp.getAdminDirPaths()));
                }
            }
        }
    }

    /**
     * Only to be called for AN/RN
     */
    private static void checkHelpers(Topology topo,
                                     ResourceId rId,
                                     Parameters params,
                                     Results results) {
        String helpers;
        String myNodeHostPort = null;
        if (rId instanceof RepNodeId) {
            RepNodeParams rnp = params.get((RepNodeId)rId);
            if (rnp == null) {
                return;
            }
            helpers = rnp.getJEHelperHosts();
            myNodeHostPort = rnp.getJENodeHostPort();
        } else {
             ArbNodeParams anp = params.get((ArbNodeId)rId);
             if (anp == null) {
                 return;
             }
             helpers = anp.getJEHelperHosts();
             myNodeHostPort = anp.getJENodeHostPort();
        }

        /* Find the HA node host addresses for each member of the shard. */
        Set<String> nodeHosts = getTopoHelpers(topo, rId, params);

        /*
         * Check the number of helpers. Note that a half-run plan
         * may have not have the proper number of helpers.
         */
        StringTokenizer t = new StringTokenizer(helpers, ",");
        while (t.hasMoreElements()) {
            String unoHelper = t.nextToken().trim();
            if (unoHelper.equals(myNodeHostPort)) {
                continue;
            }
            if (!nodeHosts.contains(unoHelper)) {
                /* Helper host parameters don't match topology */
                Iterator<String> nodeHost = nodeHosts.iterator();
                StringBuilder sb = new StringBuilder();
                while (nodeHost.hasNext()) {
                    if (sb.length() > 0) {
                        sb.append(", ");
                    }
                    sb.append(nodeHost.next());
                }
                String topoHelpersAsString = sb.toString();
                results.add(
                    new HelperParameters(rId,
                                         topoHelpersAsString,
                                         helpers));
            }
        }
    }

    /**
     * Only to be called for AN/RN to get
     * the list of helper hosts defined by the topology.
     * @param topo
     * @param rId
     * @return  helper hosts
     */
    public static Set<String> getTopoHelpers(Topology topo,
                                             ResourceId rId,
                                             Parameters params) {
        RepGroup rg;
        HashSet<String> nodeHosts = new HashSet<String>();
        if (rId instanceof RepNodeId) {
            RepNode rn = topo.get((RepNodeId)rId);
            rg = topo.getRepGroupMap().get(rn.getRepGroupId());
        } else {
             ArbNode an = topo.get((ArbNodeId)rId);
             rg = topo.getRepGroupMap().get(an.getRepGroupId());
        }

        /* Find the HA node host addresses for each member of the shard. */
        String thisRN = null;
        for (RepNode rn: rg.getRepNodes()) {
            RepNodeParams rnp = params.get(rn.getResourceId());
            if (rnp != null) {
                final String helper = rnp.getJENodeHostPort();
                if (rn.getResourceId().equals(rId)) {
                    thisRN = helper;
                } else {
                    nodeHosts.add(helper);
                }
            }
        }
        for (ArbNode an: rg.getArbNodes()) {
            ArbNodeParams anp = params.get(an.getResourceId());
            if (anp != null) {
                if (!an.getResourceId().equals(rId)) {
                    nodeHosts.add(anp.getJENodeHostPort());
                }
            }
        }

        if (nodeHosts.isEmpty() && (thisRN != null)) {
            nodeHosts.add(thisRN);
        }

        return nodeHosts;
    }

    /*
     * Check partition assignments. If a redistribution plan is interrupted
     * shards may have non-optimal numbers of partitions.
     */
    private static void checkPartitionLayout(Topology topo,
                                             Parameters params,
                                             Results results) {
        /*
         * Find shards that have no partitions. This flavor of createShardMap
         * retains shards that have 0 partitions.
         */
        final Map<RepGroupId, ShardDescriptor> shardMap = createShardMap(topo);
        for (ShardDescriptor sd : shardMap.values()) {
            /* Warn that this shard had no partitions at all. */
            if (sd.getNumPartitions() == 0) {
                results.add(new NoPartition(sd.getGroupId()));
            }
        }

        if (topo.getPartitionMap().isEmpty()) {
            return;
        }
        final Map<RepGroupId, ShardDescriptor> shards =
                                createShardMap(topo, params);

        /* Record the shards that are over or under target number. */
        for (ShardDescriptor sd : shards.values()) {
            if (sd.getOverUnder() != 0) {
                results.add(new NonOptimalNumPartitions
                            (sd.getGroupId(), sd.getNumPartitions(),
                             sd.getTargetNumPartitions(),
                             sd.getTargetNumPartitions()));
            }
            // TODO - check for target number 0? Maybe use that to indicate
            // the directory is too small for the store to be balanced?

            /* Check size discrepancy */
            if (sd.isSizeImbalanced()) {
                results.add(new StorageDirectorySizeImbalance(sd.getGroupId(),
                                                           sd.getMinDirSize(),
                                                           sd.getMaxDirSize()));
            }
        }
    }

    private static void checkTopoHelperRules(Topology topo,
                                             Results results,
                                             Parameters params) {
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            checkHelpers(topo, rnId, params, results);
        }
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            checkHelpers(topo, anId, params, results);
        }
    }

    private static void checkArbiterRules(Topology topo, Results results,
                                          Parameters params) {

        if (!ArbiterTopoUtils.useArbiters(topo)) {
            /*
             * Not using Arbiters so check if there are any.
             */
            findArbitersToBeDeleted(topo, results);
            return;
        }

        /*
         * Find the best primary DC that allows arbiters in the store
         */
        DatacenterId bestArbDcId =
            ArbiterTopoUtils.getBestArbiterDC(topo, params);

        /*
         * This flag is used to determine if AN distribution checks
         * should be performed. If there are other violations that
         * once corrected would change the AN distribution, then the
         * AN distribution check is not performed.
         */
        boolean checkDistribution = true;
        /*
         * Within the primary arbiter hosting datacenter, the arbiters will be
         * hosted in those SNs that allows arbiters.
         */
        HashMap<StorageNodeId, Integer> snANCountMap = new HashMap<>();
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(arbNodeId);
            StorageNodeId snId = arbNode.getStorageNodeId();
            /*
             * Check if the arbiter is hosted in the SN that does not allow
             * arbiters.
             */
            if (!params.get(snId).getAllowArbiters()) {
                checkDistribution = false;
                results.add(new ANNotAllowedOnSN(arbNodeId, snId, bestArbDcId));
            }
            Integer anCount = snANCountMap.get(snId);
            if (anCount == null) {
                anCount = Integer.valueOf(1);
            } else {
                anCount = Integer.valueOf(anCount + 1);
            }
            snANCountMap.put(snId, anCount);
        }

        /*
         * Arbiters will be hosted in a single primary DC that allows
         * ANs. Add violations for ANs hosted outside of the choosen
         * Arbiter DC.
         */
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(arbNodeId);
            StorageNodeId snId = arbNode.getStorageNodeId();
            DatacenterId dcId = topo.get(snId).getDatacenterId();
            if (dcId.equals(bestArbDcId)) {
                continue;
            }
            /*
             * Report all the arbiters that needs to be transfered
             */
            results.add(new ANWrongDC(dcId, bestArbDcId, arbNodeId));
            checkDistribution = false;
        }

        /*
         * Check and add violation if shard does not
         * have an AN.
         */
        Set<RepGroupId> shardsWithNoArbs = topo.getRepGroupIds();

        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            shardsWithNoArbs.remove(topo.get(arbNodeId).getRepGroupId());
        }

        for (RepGroupId rgId : shardsWithNoArbs) {
            /*
             * Report the repgroups with no arbiters.
             */
            results.add(new InsufficientANs(rgId, bestArbDcId));
            checkDistribution = false;
        }

        /* All RNs and AN of a single shard must be on different SNs */
        for (RepGroupId rgId : topo.getRepGroupIds()) {

            /* Organize RNIds by SNId */
            Map<StorageNodeId, Set<RepNodeId>> rnIdBySNId = new HashMap<>();

            for (RepNode rn : topo.get(rgId).getRepNodes()) {
                StorageNodeId snId = rn.getStorageNodeId();
                Set<RepNodeId> rnIds = rnIdBySNId.get(snId);
                if (rnIds == null) {
                    rnIds = new HashSet<>();
                    rnIdBySNId.put(rn.getStorageNodeId(), rnIds);
                }
                rnIds.add(rn.getResourceId());
            }

            for (ArbNodeId anId : topo.getArbNodeIds()) {
                ArbNode an = topo.get(anId);
                Set<RepNodeId> rns = rnIdBySNId.get(an.getStorageNodeId());
                if (rns != null) {
                    ArrayList<RepNodeId> rnsInGrp = null;
                    for (RepNodeId rnId : rns) {
                        RepNode rn = topo.get(rnId);
                        if (rn.getRepGroupId().equals(an.getRepGroupId())) {
                            if (rnsInGrp == null) {
                                rnsInGrp = new ArrayList<>();
                            }
                            rnsInGrp.add(rnId);
                        }
                    }
                    if (rnsInGrp != null) {
                        results.add(new ANProximity(an.getStorageNodeId(),
                                    rgId,
                                    rnsInGrp,
                                    anId));
                        checkDistribution = false;
                    }
                }
            }
        }

        /* Check for more than one AN in a RepGroup. */
        for (final RepGroup rg : topo.getRepGroupMap().getAll()) {
            boolean found = false;
            for (final ArbNode an : rg.getArbNodes()) {
                if (!found) {
                    found = true;
                } else {
                    results.add(new ExcessANs(rg.getResourceId(),
                                              an.getResourceId()));
                    checkDistribution = false;
                }
            }
        }

        /* Make checks for AN distribution */

        if (!checkDistribution) {
            /*
             * Skip the AN distribution check if there are violations
             * that if corrected would change the AN distribution.
             */
            return;
        }

        DatacenterId anDCId = null;
        for (StorageNodeId snId : snANCountMap.keySet()) {
            if (anDCId == null) {
                anDCId = topo.get(snId).getDatacenterId();
            } else if (!topo.get(snId).getDatacenterId().equals(anDCId)) {
                return;
            }
        }
        int maxNumAN =
            ArbiterTopoUtils.computeZoneMaxANsPerSN(topo, params, anDCId);
        if (maxNumAN == 0) {
            /* No usable SNs */
            return;
        }
        for (Entry<StorageNodeId, Integer> entry : snANCountMap.entrySet()) {
            if (entry.getValue() > maxNumAN) {
                results.add(new UnevenANDistribution(entry.getKey(),
                                                     entry.getValue(),
                                                     maxNumAN));
            }
        }
    }

    /**
     * Check for all the repgroups if they contain an arbiter when RF>2.
     */
    private static void findArbitersToBeDeleted(Topology topo,
                                                Results results) {
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(arbNodeId);
            RepGroupId rgId = arbNode.getRepGroupId();
            results.add(new ExcessANs(rgId, arbNodeId));
        }
    }

    private static void checkEmptyZone(Topology topo, Results results) {
        /*
         * Set containing all the zones in the topology.
         */
        Set<Datacenter> zoneSet = new HashSet<>();
        zoneSet.addAll(topo.getDatacenterMap().getAll());

        /*
         * Iterate over all storage nodes. Remove the zones from the zoneSet
         * which has atleast one SN.
         */
        for (StorageNode sn : topo.getStorageNodes(null)) {
            zoneSet.remove(topo.get(sn.getDatacenterId()));
        }
        /*
         * Report the Empty Zones.
         */
        for (Datacenter dc : zoneSet) {
            results.add(new EmptyZone(dc.getResourceId()));
        }
    }

    private static void checkRepNodeTypes(Topology topo,
                                          Parameters params,
                                          Results results) {
        for (final RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (final RepNode rn : rg.getRepNodes()) {
                final RepNodeId rnId = rn.getResourceId();
                final RepNodeParams rnp = params.get(rnId);

                /*
                 * We expect each RN to have parameters, but they might not
                 * temporarily, due to a race condition.  There can only be an
                 * incorrect node type if there are parameters, so there is
                 * nothing more to check if the parameters are missing.
                 */
                if (rnp == null) {
                    continue;
                }

                final NodeType nodeType = rnp.getNodeType();
                final Datacenter zone =
                    topo.getDatacenter(rn.getStorageNodeId());
                final NodeType defaultNodeType =
                    Datacenter.ServerUtil.getDefaultRepNodeType(zone);
                if (nodeType != defaultNodeType) {
                    results.add(new WrongNodeType(rnId,
                                                  nodeType,
                                                  zone.getResourceId(),
                                                  zone.getDatacenterType()));
                }
            }
        }
    }

    /**
     * Checks that each zone has at least RF number of Admins. Also checks
     * if the type of the deployed Admins matches the type of the zone.
     */
    private static void checkAdmins(Topology topo,
                                    Parameters params,
                                    Results results) {
        /* Map of zone:# of Admins in that zone */
        final Map<DatacenterId, Integer> adminMap = new HashMap<>();
        for (AdminParams ap : params.getAdminParams()) {
            final StorageNode sn =
                    topo.getStorageNodeMap().get(ap.getStorageNodeId());

            /* Admin parameters pointing to a non-existent SN */
            if (sn == null) {
                results.add(new BadAdmin(ap.getAdminId(),
                                         ap.getStorageNodeId()));
                continue;
            }
            final Datacenter dc = topo.get(sn.getDatacenterId());
            Integer count = adminMap.get(dc.getResourceId());
            if (count == null) {
                count = 0;
            }
            count++;
            adminMap.put(dc.getResourceId(), count);

            final AdminType adminType = ap.getType();
            final DatacenterType zoneType = dc.getDatacenterType();

            /* Check that the Admin type matches the zone type */
            if (!(adminType.isPrimary() && zoneType.isPrimary() ||
                  adminType.isSecondary() && zoneType.isSecondary())) {
                /* Admin - zone type missmatch */
                results.add(new WrongAdminType(ap.getAdminId(), ap.getType(),
                                               dc.getResourceId(),
                                               dc.getDatacenterType()));
            }
        }

        /* Check the number of Admins in each zone */
        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            final int zoneRF = dc.getRepFactor();
            final Integer numAdmins = adminMap.get(dc.getResourceId());
            final int difference = (numAdmins == null) ? -zoneRF :
                                                         numAdmins - zoneRF;
            /* Not enough - violation */
            if (difference < 0) {
                results.add(new InsufficientAdmins(dc.getResourceId(),
                                                   zoneRF, -difference));
                continue;
            }
            /* Too many - note */
            if (difference > 0) {
                results.add(new ExcessAdmins(dc.getResourceId(), zoneRF,
                                             difference));
                continue;
            }
            /* numAdmins == zoneRF - just right */
        }
    }

    private static void findMultipleRNsInRootDir(Topology topo,
                                                 Parameters params,
                                                 Results results) {
        /*
         * Highlight SNs which have multiple RNs in the root directory. This
         * validation step is only possible when validate has been called on a
         * deployed topology. Before a deployment, the RepNodeParams mountPoint
         * field is not initialized, and the RepNodeParams themselves may not
         * exist.
         */
        Map<StorageNodeId, List<RepNodeId>> rnsOnRoot = new HashMap<>();

        for (RepNode rn: topo.getSortedRepNodes()) {
            StorageNodeId snId = rn.getStorageNodeId();
            RepNodeId rnId = rn.getResourceId();

            RepNodeParams rnp = params.get(rnId);
            if (rnp == null) {
                continue;
            }

            if (rnp.getStorageDirectoryPath() == null) {
                List<RepNodeId> onRoot = rnsOnRoot.get(snId);
                if (onRoot == null) {
                    onRoot = new ArrayList<>();
                    rnsOnRoot.put(snId, onRoot);
                }
                onRoot.add(rnId);
            }
        }

        for (Map.Entry<StorageNodeId, List<RepNodeId>> snRoot :
                 rnsOnRoot.entrySet()) {
            if (snRoot.getValue().size() == 1) {
                /* 1 RN on the root, no issue with that. */
                continue;
            }
            StorageNodeId snId = snRoot.getKey();
            results.add(new MultipleRNsInRoot(snId,
                                            snRoot.getValue(),
                                            params.get(snId).getRootDirPath()));
        }
    }

    /*
     * For SNs that have a memory_mb param value, check that the heap of the
     * RNs hosted on that SN don't exceed the SN memory.
     */
    private static void checkMemoryUsageOnSN(Map<StorageNodeId,
                                             Set<RepNodeId>> rnsPerSN,
                                             Parameters params,
                                             Results results) {

        for (Map.Entry<StorageNodeId, Set<RepNodeId>> entry :
            rnsPerSN.entrySet()) {
            StorageNodeId snId = entry.getKey();
            StorageNodeParams snp = params.get(snId);
            if (snp.getMemoryMB() == 0) {
                continue;
            }

            long totalRNHeapMB = 0;
            Set<RepNodeId> rnIds = entry.getValue();

            for (RepNodeId rnId : rnIds) {
                RepNodeParams rnp = params.get(rnId);
                if (rnp == null) {
                    continue;
                }
                totalRNHeapMB += params.get(rnId).getMaxHeapMB();
            }

            if (totalRNHeapMB > snp.getMemoryMB()) {
                StringBuilder sb = new StringBuilder();
                for (RepNodeId rnId : rnIds) {
                    sb.append(rnId).append(" ").
                    append(params.get(rnId).getMaxHeapMB()).append("MB,");
                }
                results.add(new RNHeapExceedsSNMemory(snId, snp.getMemoryMB(),
                                                      rnIds, totalRNHeapMB,
                                                      sb.toString()));
            }
        }
    }

    /**
     * Check that a transition from source to candidate topology is permitted:
     * <ul>
     * <li> All referenced SNs are still present
     * <li> All storage and RN log directories exist
     * <li> Once deployed, the number of partitions in a store cannot change
     * <li> A shard cannot be concurrently involved in a partition migration
     *      AND have an RN that is being moved from one SN to another.
     *      TODO: move to task generator class
     * <li> Except during a failover, the RF of a primary zone cannot be
     *      reduced and the overall primary RF cannot be reduced
     * </ul>
     */
    public static void validateTransition(Topology source,
                                          TopologyCandidate candidate,
                                          Parameters params,
                                          boolean forFailover) {

        Topology target = candidate.getTopology();

        /*
         * Make sure that all the SNs and storage directories in the candidate
         * still exist. It's possible that the user has removed them.
         */
        for (StorageNodeId snId : target.getStorageNodeIds()) {
            if (params.get(snId) == null) {
                throw new IllegalCommandException
                    ("Topology candidate " + candidate.getName() + " uses " +
                     snId + " but " + snId + " has been removed.");
            }
        }

        checkAllStorageDirsExist(candidate, params);

        /*
         * Make sure that all RN log directories in the candidate
         * still exist. It is possible that user has removed them.
         * Although currently we do not support change/remove rnlogdir option
         * but its in TODO.
         */
        checkAllRNLogDirsExist(candidate, params);

        /*
         * TODO : Need to check for admindir existence before transition.
         * Admin dir need to be part of topology candidate then.
         */

        if (source.getRepGroupMap().isEmpty()) {
            /*
             * Right now, if there are no shards in the source, there is
             * nothing else to check in the transition.
             */
            return;
        }

        /*
         * If the store has already been deployed, check that the number of
         * partitions has not changed.
         */

        Collection<PartitionId> sourcePartitionIds =
            source.getPartitionMap().getAllIds();
        Collection<PartitionId> targetPartitionIds =
            target.getPartitionMap().getAllIds();

        /* The source and target must have the same number of partitions. */
        if ((!sourcePartitionIds.isEmpty()) &&
            (sourcePartitionIds.size() != targetPartitionIds.size())) {
            throw new IllegalCommandException
                ("The current topology has been initialized and has " +
                 sourcePartitionIds.size() +
                 " partitions while the target (" + candidate.getName() +
                 ")has " + targetPartitionIds.size() +
                 " partitions. The number of partitions in a store cannot be"+
                 " changed");
        }

        /* The source and target must have the same set of partitions. */
        Set<PartitionId> mustExistCopy = new HashSet<>(sourcePartitionIds);
        mustExistCopy.removeAll(targetPartitionIds);
        if (!mustExistCopy.isEmpty()) {
            throw new IllegalCommandException
                ("These partitions are missing from " +
                 candidate.getName() + ":" + mustExistCopy);
        }

        /*
         * In the two operations adding RNs and removing RNs, only one of them
         * is supported at same time.
         */
        Set<RepNodeId> sourceRNsCopy = new HashSet<>(source.getRepNodeIds());
        Set<RepNodeId> targetRNsCopy = target.getRepNodeIds();

        sourceRNsCopy.removeAll(target.getRepNodeIds());
        targetRNsCopy.removeAll(source.getRepNodeIds());
        if (!sourceRNsCopy.isEmpty() && !targetRNsCopy.isEmpty()) {
            throw new IllegalCommandException
                ("Adding Rep Nodes and Removing Rep Nodes are not supported " +
                 "at the same time in the " + candidate.getName() + ". " +
                 "Adding Rep Nodes: " + targetRNsCopy + ", " +
                 "removing Rep Nodes: " + sourceRNsCopy);
        }

        if (!forFailover) {
            checkPrimaryRFChanges(source, candidate);
        }
    }

    public static void checkAllStorageDirsExist(TopologyCandidate candidate,
                                                Parameters params) {
        for (Map.Entry<RepNodeId, String> entry :
                 candidate.getDirectoryAssignments().entrySet()) {
            final RepNodeId rnId = entry.getKey();
            final String path = entry.getValue();
            final StorageNodeId snId =
                candidate.getTopology().get(rnId).getStorageNodeId();
            if ((path != null) &&
                !directoryExists(path,
                                  params.get(snId).getStorageDirPaths())) {
                throw new IllegalCommandException
                    ("Topology candidate " + candidate.getName() +
                     " uses storage directory " + snId + ", " + path +
                     " but it has been removed.");
            }
        }
    }

    public static void checkAllRNLogDirsExist(TopologyCandidate candidate,
                                              Parameters params) {
        for (Map.Entry<RepNodeId, String> entry :
                 candidate.getRNLogDirectoryAssignments().entrySet()) {
            final RepNodeId rnId = entry.getKey();
            final String path = entry.getValue();
            final StorageNodeId snId =
                candidate.getTopology().get(rnId).getStorageNodeId();
            if ((path != null) &&
                !directoryExists(path,
                                 params.get(snId).getRNLogDirPaths())) {
                throw new IllegalCommandException
                    ("Topology candidate " + candidate.getName() +
                     " uses RN log directory " + snId + ", " + path +
                     " but it has been removed.");
            }
        }
    }

    private static boolean directoryExists(String storageDir,
                                           List<String> sdList) {
        for (String sd : sdList) {
            if (sd.equals(storageDir)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the changes to the replication factors for primary zones are
     * permitted.
     *
     * @param source the base topology
     * @param candidate the proposed new topology
     * @throws IllegalCommandException if a problem is found
     */
    public static void checkPrimaryRFChanges(Topology source,
                                             TopologyCandidate candidate) {

        /* Compute total primary RF in source topo */
        int sourcePrimaryRF = 0;
        for (Datacenter dc : source.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isPrimary()) {
                sourcePrimaryRF += dc.getRepFactor();
            }
        }

        /*
         * Compute total primary RF in the target topo and check that the RF of
         * individual primary zones has not been reduced
         */
        final Topology target = candidate.getTopology();
        int primaryRF = 0;
        for (Datacenter dc : target.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isPrimary()) {
                final int rf = dc.getRepFactor();
                primaryRF += rf;
                final DatacenterId dcId = dc.getResourceId();
                final Datacenter sourceDC = source.get(dcId);
                if ((sourceDC != null) &&
                    sourceDC.getDatacenterType().isPrimary() &&
                    (rf < sourceDC.getRepFactor())) {
                    throw new IllegalCommandException(
                        "Attempt to reduce the replication factor of primary" +
                        " zone " + dcId +
                        " from " + sourceDC.getRepFactor() +
                        " to " + rf + ", which is not permitted.");
                }
            }
        }

        /* Check that the total primary RF has not been reduced */
        if (primaryRF < sourcePrimaryRF) {
            throw new IllegalCommandException(
                "Attempt to reduce the overall primary replication factor" +
                " by " + (sourcePrimaryRF - primaryRF) + " from " +
                sourcePrimaryRF + " to " + primaryRF +
                ". Reducing the primary replication factor is not permitted" +
                " unless performing a failover.");
        }
    }

    /**
     * Check that the replication factor is in the valid range.
     *
     * @throws IllegalCommandException if repFactor is not in the valid range
     */
    public static void validateReplicationFactor(final int repFactor) {
        if (repFactor < 0 || repFactor > MAX_REPLICATION_FACTOR) {
            throw new IllegalCommandException
                ("Illegal replication factor: " + repFactor +
                 ", valid range is 0 to " + MAX_REPLICATION_FACTOR);
        }
    }

    /**
     * Check that the rep factor for a new zone is in a valid range and is
     * permitted given the zone type and whether the zone allows arbiters.
     *
     * @throws IllegalCommandException if repFactor is not in the valid range,
     * or if the replication factor is 0, the zone type is primary, and
     * allowArbiters is false
     */
    public static void validateReplicationFactor(final int repFactor,
                                                 final DatacenterType zoneType,
                                                 final boolean allowArbiters) {
        validateReplicationFactor(repFactor);
        if ((repFactor == 0) && zoneType.isPrimary() && !allowArbiters) {
            throw new IllegalCommandException(
                "Replication factor 0 is not permitted on primary zones" +
                " that don't allow arbiters. Replication factor 0 is only" +
                " permitted for secondary zones, or for primary zones that" +
                " allow arbiters.");
        }
    }

    /**
     * Check that the new rep factor for an existing zone is in the valid range
     * and is permitted given the zone characteristics.
     *
     * @throws IllegalCommandException if the replication factor is not in the
     * valid range, or if the zone is a primary zone and the new replication
     * factor is lower than the current one
     */
    public static void validateChangeReplicationFactor(int repFactor,
                                                       Datacenter zone) {
        validateReplicationFactor(repFactor);
        if (zone.getDatacenterType().isPrimary() &&
            (repFactor < zone.getRepFactor())) {
            throw new IllegalCommandException(
                "The replication factor of a primary zone cannot be made" +
                " smaller. The current replication factor for zone " +
                zone.getResourceId() + " is " + zone.getRepFactor() +
                ", the requested replication factor was " + repFactor + ".");
        }
    }

    /**
     * Check that the value of allowArbiters is valid given the zone type and
     * replication factor.
     *
     * @throws IllegalCommandException if allowArbiters is true and the zone
     * type is not primary, or if allowArbiters is false, the zone type is
     * primary, and repFactor is 0
     */
    public static void validateAllowArbiters(final boolean allowArbiters,
                                             final DatacenterType dt,
                                             final int repFactor) {
        if (allowArbiters && !dt.isPrimary()) {
            throw new IllegalCommandException(
                "Allowing arbiters is not permitted on secondary zones." +
                " Arbiters are only allowed on primary zones.");
        }
        if (!allowArbiters && dt.isPrimary() && (repFactor == 0)) {
            throw new IllegalCommandException(
                "Allowing arbiters is required on primary zones with" +
                " replication factor 0. Not allowing arbiters is only" +
                " permitted on secondary zones or on primary zones with a" +
                " replication factor greater than 0.");
        }
    }

    /**
     * Check that the value of masterAffinity is valid given the zone type and
     * replication factor.
     *
     * @throws IllegalCommandException if masterAffinity is true, and either
     * zoneType is not primary, or it is primary and repFactor is 0
     */
    public static void validateMasterAffinity(final boolean masterAffinity,
                                              final DatacenterType zoneType,
                                              final int repFactor) {
        if (masterAffinity && (!zoneType.isPrimary() || (repFactor == 0))) {
            throw new IllegalCommandException(
                "Master affinity is not allowed for " +
                (!zoneType.isPrimary() ?
                 "secondary zones." :
                 "primary zones with replication factor 0.") +
                " Master affinity is only allowed for primary zones whose" +
                " replication factor is not 0.");
        }
    }

    /**
     * Check if "rn" can be placed on this SN. Account for constraints that can
     * be considered in the scope of a single SN, like capacity and RN
     * consanguinity.
     */
    static boolean checkRNPlacement(StorageNodeParams snp,
                                    Set<RepNodeId> assignedRNs,
                                    int proposedRG,
                                    boolean ignoreCapacity,
                                    Logger logger) {

        if (!ignoreCapacity &&
            (assignedRNs.size() >= snp.getCapacity())) {
            logger.log(Level.FINEST, "{0} capacity={1}, num hosted RNS={2}",
                       new Object[]{snp.getStorageNodeId(), snp.getCapacity(),
                                    assignedRNs.size()});
            return false;
        }

        for (RepNodeId r : assignedRNs) {
            if (r.getGroupId() == proposedRG) {
                logger.log(Level.FINEST,
                           "{0} already has RN {1} from same shard as {2}",
                           new Object[]{snp.getStorageNodeId(), r, proposedRG});
                return false;
            }
        }

        return true;
    }

    /**
     * Return the relative complement of A and B. That is,return all the items
     * in A that are not also in B.
     */
    private static Set<RepGroupId> relComplement(Set<RepGroupId> a,
                                                 Set<RepGroupId> b) {

        Set<RepGroupId> copyA = new HashSet<>(a);
        copyA.removeAll(b);
        return copyA;
    }

    /**
     * Checks whether all or no storage directories have sizes. Returns an empty
     * set of all storage directories have sizes or none do. Otherwise, the set
     * of SNs which have storage directories that are missing sizes. The union
     * set of SNs in the specified topology as well as the pool are checked.
     *
     * If an SN does not have any explicit storage directories, or the capacity
     * is greater than the number of explicit storage directories, the size of
     * the root directory is checked.
     *
     * @return the set of SNs that are missing storage directory sizes
     */
    static Set<StorageNodeId> validateStorageDirSizes(Topology topo,
                                                      StorageNodePool pool,
                                                      Parameters params) {
        final Set<StorageNodeId> allSNs = new HashSet<>(pool.getList());
        allSNs.addAll(topo.getStorageNodeIds());

        final Set<StorageNodeId> noSizes = new HashSet<>();
        boolean sizeSpecified = false;
        for (StorageNodeId snId : allSNs) {
            final StorageNodeParams snp = params.get(snId);
            final ParameterMap sdMap = snp.getStorageDirMap();
            // TODO can sdMap be null?
            final int numStorageDirs = (sdMap == null) ? 0 : sdMap.size();

            /*
             * If the capacity is > the number of defined storage directories
             * check whether the root size was specified.
             */
            if (snp.getCapacity() > numStorageDirs) {
                if (snp.getRootDirSize() > 0) {
                    sizeSpecified = true;
                } else {
                    noSizes.add(snId);
                }
            }

            if (sdMap != null) {
                for (Parameter p : sdMap) {
                    if (SizeParameter.getSize(p) > 0) {
                        sizeSpecified = true;
                    } else {
                        noSizes.add(snId);
                    }
                }
            }
        }
        if (!sizeSpecified) {
            noSizes.clear();
        }
        return noSizes;
    }

    /**
     * Checks whether all or no RN log directories have sizes. Returns an empty
     * set of all RN log directories have sizes or none do. Otherwise, the set
     * of SNs which have RN log directories that are missing sizes. The union
     * set of SNs in the specified topology as well as the pool are checked.
     *
     * This code is currently unused and will be added in topology
     * builder when rnlogdirsize will be exposed.
     *
     * @return the set of SNs that are missing RN log directory sizes
     */
    static Set<StorageNodeId> validateRNLogDirSizes(Topology topo,
                                                    StorageNodePool pool,
                                                    Parameters params) {
        final Set<StorageNodeId> allSNs = new HashSet<>(pool.getList());
        allSNs.addAll(topo.getStorageNodeIds());

        final Set<StorageNodeId> noSizes = new HashSet<>();
        boolean sizeSpecified = false;
        for (StorageNodeId snId : allSNs) {
            final StorageNodeParams snp = params.get(snId);
            final ParameterMap rnlMap = snp.getRNLogDirMap();

            if (rnlMap != null) {
                for (Parameter p : rnlMap) {
                    if (SizeParameter.getSize(p) > 0) {
                        sizeSpecified = true;
                    } else {
                        noSizes.add(snId);
                    }
                }
            }
        }
        if (!sizeSpecified) {
            noSizes.clear();
        }
        return noSizes;
    }

    /**
     * Creates a map of shard descriptors based on the specified topology
     * candidate. Shard minimum directory sizes will be calculated for each
     * shard based on information from the parameters and candidate. If the
     * candidate's topology is uninitialized an empty map is returned.
     */
    static SortedMap<RepGroupId, ShardDescriptor>
                                 createShardMap(TopologyCandidate candidate,
                                                Parameters params) {
        final Topology topo = candidate.getTopology();
        final int numPartitions = topo.getPartitionMap().size();
        if (topo.getRepGroupMap().isEmpty() || (numPartitions == 0)) {
            return new TreeMap<>();
            // For when we can use Java 8
            //return Collections.emptySortedMap();
        }
        final SortedMap<RepGroupId, ShardDescriptor> shards =
                                                        createShardMap(topo);

        /* Collect directory size information for the nodes in each shard */
        collectDirSizes(shards, candidate, topo, params);
        return shards;
    }

    /**
     * Creates a map of shard descriptors based on the specified topology
     * candidate. Shard minimum directory sizes and target partition
     * numbers will be calculated for each shard from the candidate's
     * storage directory information. Shards specified in
     * removedRgIds will be configured for removal.
     */
    static SortedMap<RepGroupId, ShardDescriptor>
                                 createShardMap(TopologyCandidate candidate,
                                                Parameters params,
                                                List<RepGroupId> removedRgIds,
                                                int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Invalid number of partitions: "+
                                               numPartitions);
        }
        final Topology topo = candidate.getTopology();
        if (topo.getRepGroupMap().isEmpty()) {
            throw new IllegalStateException("Attempt to create shard map from "+
                                            "uninitialized topology");
        }
        final SortedMap<RepGroupId, ShardDescriptor> shards =
                                                          createShardMap(topo);

        /* Collect directory size information for the nodes in each shard */
        collectDirSizes(shards, candidate, topo, params);

        /* Mark shards to be removed */
        for (RepGroupId rgId : removedRgIds) {
            shards.get(rgId).targetForRemoval();
        }
        return setPartitionTargets(topo, shards, numPartitions);
    }

    /**
     * Creates a map of shard descriptors based on the specified topology.
     * Shard minimum directory sizes and target partition numbers will be
     * calculated for each shard based on information from the parameters.
     * If the topology is uninitialized an empty map is returned.
     */
    private static SortedMap<RepGroupId, ShardDescriptor>
                                 createShardMap(Topology topo,
                                                Parameters params) {
        final int numPartitions = topo.getPartitionMap().size();
        if (topo.getRepGroupMap().isEmpty() || (numPartitions == 0)) {
            return new TreeMap<>();
            // For when we can use Java 8
            //return Collections.emptySortedMap();
        }
        final SortedMap<RepGroupId, ShardDescriptor> shards =
                                                        createShardMap(topo);

        /* Collect directory size information for the nodes in each shard */
        collectDirSizes(shards, null, topo, params);

        return setPartitionTargets(topo, shards, numPartitions);
    }

    /**
     * Creates a map of shard descriptors based on the specified topology. The
     * shard descriptors are only initialized with partition counts if
     * available.
     */
    private static SortedMap<RepGroupId, ShardDescriptor>
                                                 createShardMap(Topology topo) {
        /* Create descriptor for each shard */
        final SortedMap<RepGroupId, ShardDescriptor> shards = new TreeMap<>();
        for (RepGroupId rgId : topo.getRepGroupIds()) {
            shards.put(rgId, new ShardDescriptor(rgId));
        }

        /* Populate the assigned partitions */
        for (Partition p: topo.getPartitionMap().getAll()) {
            shards.get(p.getRepGroupId()).addPartition(p.getResourceId());
        }
        return shards;
    }

    /**
     * Collects directory size information for the nodes in each shard.  If
     * candidate is not null the candidate assignments are checked first for
     * a directory path otherwise the path is obtained from the rep node
     * parameters. The directory size is obtained from the storage node
     * parameters. (see TopologyCandidate.getStorageDir)
     */
    private static void collectDirSizes(
                                 SortedMap<RepGroupId, ShardDescriptor> shards,
                                 TopologyCandidate candidate,
                                 Topology topo,
                                 Parameters params) {
        for (RepNode rn : topo.getSortedRepNodes()) {
            final long size =
                    TopologyCandidate.getStorageDir(rn.getResourceId(),
                                                    candidate,
                                                    topo, params).getSize();
            shards.get(rn.getRepGroupId()).setDirSize(size);
        }
    }

    /**
     * Sets the target number of partitions in the shard descriptors. The
     * modified map of descriptors is returned.
     *
     * Note that the algorithm below assumes that the directory sizes are
     * much larger than the number of partitions, at least an order of
     * magnitude or two. In practice the difference will be many orders so
     * not an issue. The problem may arise during testing where someone uses
     * a small number for a directory size. In this case the partition
     * distribution could be a little off due to rounding errors.
     */
    private static SortedMap<RepGroupId, ShardDescriptor>
              setPartitionTargets(Topology topo,
                                  SortedMap<RepGroupId, ShardDescriptor> shards,
                                  int numPartitions) {
        /*
         * Find the total size available for the
         * store as well as the smallest shard.
         */
        long totalDirSize = 0L;
        long smallestDirSize = Long.MAX_VALUE;

        for (ShardDescriptor sd : shards.values()) {
            if (sd.isTargetForRemoval()) {
                continue;
            }
            long minDirSize = sd.getMinDirSize();
            totalDirSize += minDirSize;
            if (minDirSize < smallestDirSize) {
                smallestDirSize = minDirSize;
            }
        }

        /* This should allready be checked, but check again */
        if ((totalDirSize > 0) && (smallestDirSize <= 0)) {
            throw new IllegalCommandException("not all storage directories " +
                                              "have sizes");
        }

        /* The minimum # of partitions per shard */
        int minNumPartitions;

        /* The amount of directory needed for one partition */
        long bytesPerPartition = 0L;

        if (totalDirSize == 0) {
            final int totalShards = topo.getRepGroupMap().size();

            /*
             * We don't know anything, so evenly distribute partitions across
             * shards
             */
            minNumPartitions = numPartitions / totalShards;
        } else {
            bytesPerPartition = totalDirSize / numPartitions;
            minNumPartitions = (bytesPerPartition > 0) ?
                            (int)(smallestDirSize / bytesPerPartition) : 0;
        }

        /* If calculations end up at 0, set to at least 1 partition per shard */
        if (minNumPartitions == 0) {
            minNumPartitions = 1;
        }

        int totalAssigned = 0;
        for (ShardDescriptor sd : shards.values()) {
            totalAssigned +=
                     sd.setTargetNumPartitions(minNumPartitions,
                                               bytesPerPartition,
                                               numPartitions - totalAssigned);
            // TODO - how about checking for 0 meaning the directory was too small
            if (totalAssigned >= numPartitions) {
                break;
            }
        }

        /* Assign remaining partitions by spreading them across shards */
        while (totalAssigned < numPartitions) {
            for (ShardDescriptor sd : shards.values()) {
                if (sd.isTargetForRemoval()) {
                    continue;
                }
                sd.addTargetNumPartition();
                totalAssigned++;
                if (totalAssigned >= numPartitions) {
                    break;
                }
            }
        }
        if (totalAssigned != numPartitions) {
            throw new IllegalStateException("Internal error calculating " +
                                            "partition targets");
        }
        return shards;
    }

    /**
     * Shard descriptor. When inserted into a TreeSet the descriptors will sort
     * in order of the number of partitions, least to most.
     */
    static class ShardDescriptor implements Comparable<ShardDescriptor> {
        private final RepGroupId rgId;
        private final List<PartitionId> partitions = new ArrayList<>();
        /* Set to -1 if the shard is being removed */
        private int targetNumPartitions = 0;

        /* Size of the smallest directory in this shard */
        private long minDirSize = Long.MAX_VALUE;

        /* Size of the largest directory in this shard */
        private long maxDirSize = 0L;

        /* True if the shard is directory size imbalanced. */
        private boolean sizeImbalance = false;

        /* Constructor. The partition list will be empty. */
        private ShardDescriptor(RepGroupId rgId) {
            this.rgId = rgId;
        }

        RepGroupId getGroupId() {
            return rgId;
        }

        private int getNumPartitions() {
            return partitions.size();
        }

        int getTargetNumPartitions() {
            return isTargetForRemoval() ? 0 : targetNumPartitions;
        }

        PartitionId removePartition() {
            return partitions.isEmpty() ? null : partitions.remove(0);
        }

        void addPartition(PartitionId partitionId) {
            partitions.add(partitionId);
        }

        /**
         * Returns the number of partitions over or under the target number
         * of partitions. If over the value will be positive. If under the
         * value will be negative. If the number of partitions equals the
         * target number zero is returned.
         *
         * @return the number of partitions over or under the target number
         */
        int getOverUnder() {
            return  partitions.size() - getTargetNumPartitions();
        }

        /*
         * If targeted for removal we set the target partitions to -1 and set
         * the directory size to 0 so that it is not counted.
         */
        private void targetForRemoval() {
            minDirSize = 0L;
            maxDirSize = 0L;
            targetNumPartitions = -1;
        }

        /**
         * Returns true if targetForRemoval() has been called.
         */
        boolean isTargetForRemoval() {
            return targetNumPartitions < 0;
        }

        /**
         * Sets the min and max directory size for this shard.
         */
        private void setDirSize(long size) {
            assert !isTargetForRemoval();
            if (size < minDirSize) {
                minDirSize = size;
            }
            if (size > maxDirSize) {
                maxDirSize = size;
            }
        }

        /**
         * Returns the minimum directory size for this shard. If the minimum
         * has not been initialized 0 is returned.
         *
         * @return the minimum directory size for this shard
         */
        long getMinDirSize() {
            return (minDirSize < Long.MAX_VALUE) ? minDirSize : 0L;
        }

        /**
         * Returns the maximum directory size for this shard. If the maximum
         * has not been initialized 0 is returned.
         *
         * @return the maximum directory size for this shard
         */
        private long getMaxDirSize() {
            return maxDirSize;
        }

        /**
         * Returns true if the shard is directory size imbalanced.
         */
        private boolean isSizeImbalanced() {
            return sizeImbalance;
        }

        /**
         * Increments the target number of partitions by one. The resulting
         * value is returned.
         *
         * @return the new target number of partitions.
         */
        private int addTargetNumPartition() {
            assert !isTargetForRemoval();
            targetNumPartitions++;
            return targetNumPartitions;
        }

        /**
         * Sets the target number of partitions based on the input parameters.
         * The target is based on the directory size divided by
         * bytesPerPartition. If the directory size is 0, or the calculation
         * results in a value less than 1, the target is set to
         * minNumPartitions. If the shard is marked for removal the target
         * is set to 0. The target value is returned.
         *
         * @param minNumPartitions min partitions
         * @param bytesPerPartition the target size for each partition
         * @param maxPartitions the maximum partitions that can be assigned
         * @return the target number of partitions
         */
        private int setTargetNumPartitions(int minNumPartitions,
                                           long bytesPerPartition,
                                           int maxPartitions) {
            assert minNumPartitions > 0;

            if (isTargetForRemoval()) {
                return 0;
            }
            targetNumPartitions = minNumPartitions;
            if ((minDirSize > 0L) && (bytesPerPartition > 0L)) {
                final int np = (int)(minDirSize / bytesPerPartition);

                if (np > minNumPartitions) {
                    targetNumPartitions = np;
                }

                /*
                 * The shard is imbalanced if it could host more partitions
                 * if all nodes were as large as the max.
                 */
                final long sizeDiff = maxDirSize - minDirSize;
                sizeImbalance = (int)(sizeDiff / bytesPerPartition) > 0;
            }
            if (targetNumPartitions > maxPartitions) {
                targetNumPartitions = maxPartitions;
            }
            return targetNumPartitions;
        }

        /* Sort by who is over the most */
        @Override
        public int compareTo(ShardDescriptor gd) {
            if (this.equals(gd)) {
                return 0;
            }
            return (getOverUnder() >= gd.getOverUnder()) ? 1 : -1;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder(), "").toString();
        }

        StringBuilder toString(StringBuilder sb, String tab) {
            sb.append(tab).append("ShardDescriptor[").append(rgId);
            sb.append(" partitions=").append(partitions.size());
            sb.append(" targetPartitions=").append(targetNumPartitions);
            sb.append(" minDirSize=").append(minDirSize);
            sb.append(" sizeImbalance=").append(sizeImbalance);
            sb.append("]");
            return sb;
        }
    }

    /**
     * Create a profile which contains over and under utilized SNs, capacity
     * problems and warnings, and the excessive capacity count for a topology.
     *
     * This does not account for datacenter topology (one datacenter may gate
     * the others in terms of how many shards can be supported, so some RN
     * slots in a given datacenter may intentionally be unused), so this method
     * would have to be updated when there is datacenter support.
     */
    static CapacityProfile getCapacityProfile(Topology topo,
                                              Parameters params) {

        Map<StorageNodeId,Integer> rnCount = new HashMap<>();

        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            rnCount.put(snId, 0);
        }

        for (RepNode rn: topo.getSortedRepNodes()) {
            StorageNodeId snId = rn.getStorageNodeId();
            int currentCount = rnCount.get(snId);
            rnCount.put(snId, ++currentCount);
        }

        CapacityProfile profile = new CapacityProfile();
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            final int count = rnCount.get(snId);
            final StorageNodeParams snParams = params.get(snId);

            /*
             * snParams may be null if the Storage Node(s) does not exist
             * in the current store. Might be removed by the user earlier.
             */
            if (snParams == null) {
                continue;
            }
            final int capacity = snParams.getCapacity();
            profile.noteUtilization(snId, capacity, count);
        }
        return profile;
    }

    /**
     * Packages information about the capacity characteristics of a topology.
     * List SNs that are over or under utilized, and capacity violations and
     * warnings.
     *
     * Also includes an excessCapacity count, which is a measure of all unused
     * slots. Note that this count can be &lt; 0 if the SNs are under utilized.
     * Likewise, a 0 excessCapacity doesn't mean that the topology is totally
     * in compliance. It could be a result of a combination of over and under
     * capacity SNs.
     */
    static class CapacityProfile {

        private int excessCapacity;
        private final List<StorageNodeId> overUtilizedSNs;
        private final List<StorageNodeId> underUtilizedSNs;
        private final List<RulesProblem> problems;
        private final List<RulesProblem> warnings;

        CapacityProfile() {
            overUtilizedSNs = new ArrayList<>();
            underUtilizedSNs = new ArrayList<>();
            problems = new ArrayList<>();
            warnings = new ArrayList<>();
        }

        public List<RulesProblem> getWarnings() {
            return warnings;
        }

        List<RulesProblem> getProblems() {
            return problems;
        }

        /**
         * If the capacity and RN count don't match, record the discrepancy.
         */
        private void noteUtilization(StorageNodeId snId,
                                     int snCapacity,
                                     int rnCount) {
            int capacityDelta = snCapacity - rnCount;
            excessCapacity += capacityDelta;
            if (capacityDelta > 0) {
                UnderCapacity under =
                    new UnderCapacity(snId, rnCount, snCapacity);
                underUtilizedSNs.add(snId);
                warnings.add(under);
            } else if (capacityDelta < 0) {

                OverCapacity over = new OverCapacity(snId, rnCount, snCapacity);
                overUtilizedSNs.add(snId);
                problems.add(over);
            }
        }

        int getExcessCapacity() {
            return excessCapacity;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("excess=").append(excessCapacity).append("\n");
            sb.append("problems:").append(problems).append("\n");
            sb.append("warnings:").append(warnings).append("\n");
            return sb.toString();
        }
    }

    /**
     * Return the total sum capacity of all the SNs in the specified pool.
     * Do not consider whether there are any RNs assigned to those SNs yet.
     */
    static int calculateMaximumCapacity(StorageNodePool snPool,
                                        Parameters params) {

        int capacityCount = 0;
        for (StorageNodeId snId : snPool.getList()) {
            capacityCount += params.get(snId).getCapacity();
        }
        return capacityCount;
    }

    /**
     * A Boolean predicate for rules problems.
     */
    public interface RulesProblemFilter<T extends RulesProblem> {

        /**
         * Returns whether the problem matches the predicate.
         */
        boolean match(T problem);
    }

    public static class Results {
        private final List<RulesProblem> violations;
        private final List<RulesProblem> warnings;

        Results() {
            violations = new ArrayList<>();
            warnings = new ArrayList<>();
        }

        void add(RulesProblem p) {
            if (p.isViolation()) {
                violations.add(p);
            } else {
                warnings.add(p);
            }
        }

        public List<RulesProblem> getViolations() {
            return violations;
        }

        public List<RulesProblem> getWarnings() {
            return warnings;
        }

        /**
         * Return both violations and warnings.
         */
        public List<RulesProblem> getProblems() {
            List<RulesProblem> all = new ArrayList<>(violations);
            all.addAll(warnings);
            return all;
        }

        public int numProblems() {
            return violations.size() + warnings.size();
        }

        public int numViolations() {
            return violations.size();
        }

        /**
         * Return a list of all of this kind of problem.
         */
        public <T extends RulesProblem> List<T> find(Class<T> problemClass) {
            List<T> p = new ArrayList<>();
            for (RulesProblem rp : violations) {
                if (rp.getClass().equals(problemClass)) {
                    p.add(problemClass.cast(rp));
                }
            }

            for (RulesProblem rp : warnings) {
                if (rp.getClass().equals(problemClass)) {
                    p.add(problemClass.cast(rp));
                }
            }

            return p;
        }

        /**
         * Return a list of the specified kind of problem that match the
         * filter.
         */
        public <T extends RulesProblem> List<T> find(
            final Class<T> problemClass, final RulesProblemFilter<T> filter) {

            final List<T> p = new ArrayList<>();
            for (final RulesProblem rp : violations) {
                if (rp.getClass().equals(problemClass)) {
                    final T crp = problemClass.cast(rp);
                    if (filter.match(crp)) {
                        p.add(crp);
                    }
                }
            }

            for (final RulesProblem rp : warnings) {
                if (rp.getClass().equals(problemClass)) {
                    final T crp = problemClass.cast(rp);
                    if (filter.match(crp)) {
                        p.add(problemClass.cast(rp));
                    }
                }
            }

            return p;
        }

        @Override
        public String toString() {
            if (numProblems() == 0) {
                return "No problems";
            }

            StringBuilder sb = new StringBuilder();

            int numViolations = violations.size();
            if (numViolations > 0) {
                sb.append(numViolations);
                sb.append((numViolations == 1) ? " violation.\n" :
                          " violations.\n");

                for (RulesProblem r : violations) {
                    sb.append(r);
                    sb.append(" (");
                    sb.append(r.getClass().getSimpleName());
                    sb.append(")\n");
                }
            }

            int numWarnings = warnings.size();
            if (numWarnings > 0) {
                sb.append(numWarnings);
                sb.append((numWarnings == 1) ? " warning.\n" :
                          " warnings.\n");
                for (RulesProblem r : warnings) {
                    sb.append(r);
                    sb.append(" (");
                    sb.append(r.getClass().getSimpleName());
                    sb.append(")\n");
                }
            }

            return sb.toString();
        }


        /**
         * Find all the issues that are in this results class, but are
         * not in "other".
         */
        public Results remove(Results other) {
            Results diff = new Results();
            List<RulesProblem> pruned = new ArrayList<>(violations);
            pruned.removeAll(other.violations);
            diff.violations.addAll(pruned);

            pruned = new ArrayList<>(warnings);
            pruned.removeAll(other.warnings);
            diff.warnings.addAll(pruned);
            return diff;
        }
    }
}
