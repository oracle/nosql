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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.Rules.RulesProblemFilter;
import oracle.kv.impl.admin.topo.Rules.ShardDescriptor;
import oracle.kv.impl.admin.topo.Validations.ANNotAllowedOnSN;
import oracle.kv.impl.admin.topo.Validations.ANProximity;
import oracle.kv.impl.admin.topo.Validations.ANWrongDC;
import oracle.kv.impl.admin.topo.Validations.ExcessANs;
import oracle.kv.impl.admin.topo.Validations.ExcessRNs;
import oracle.kv.impl.admin.topo.Validations.InsufficientANs;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.RNProximity;
import oracle.kv.impl.admin.topo.Validations.UnevenANDistribution;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

/**
 * Generate a topology candidate which uses the resources provided to the
 * builder.
 * TODO: ensure that the parameters are not modified while we are building a
 * topology. Get a copy of the params?
 */
public class TopologyBuilder {

    private final TopologyCandidate sourceCandidate;
    private final StorageNodePool snPool;
    private final int numPartitions;
    private final Parameters params;
    private final Logger logger;

    /* Store to-be-removed shards ID */
    private final List<RepGroupId> removedRgIds = new ArrayList<>();

    /* The primary RF of the source topology */
    private final int totalPrimaryRF;

    /* True if arbiters are enabled in the source topology */
    private final boolean arbitersEnabled;

    /* Set in the addANs method and returned by getModifiedANCount */
    private int modifiedANs;

    /**
     * A set of string containing important notes or information for the client
     * during the topology-building process. This variable can store data
     * that may be useful for the client when constructing a specific topology.
     * Can be called wherever required during topology building.
     */
    private final Set<String> notes = new HashSet<>();

    /**
     * This constructor is for an initial deployment where the number of
     * partitions is specified by the user.
     */
    public TopologyBuilder(Topology sourceTopo,
                           String candidateName,
                           StorageNodePool snPool,
                           int numPartitions,
                           Parameters params,
                           AdminServiceParams adminParams) {
        this(new TopologyCandidate(candidateName, sourceTopo.getCopy()),
             snPool,
             numPartitions,
             params,
             adminParams,
             true);
        if (!sourceTopo.getPartitionMap().isEmpty() &&
            (sourceTopo.getPartitionMap().size() != numPartitions)) {
            throw new IllegalCommandException
                ("The number of partitions cannot be changed.",
                 ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }
    }

    /**
     * Use for an existing store, when we have to rebalance or
     * redistribute. The number of partitions in the store is fixed, and
     * determined by the initial deployment.
     */
    public TopologyBuilder(TopologyCandidate sourceCandidate,
                           StorageNodePool snPool,
                           Parameters params,
                           AdminServiceParams adminParams) {
        this(sourceCandidate,
             snPool,
             sourceCandidate.getTopology().getPartitionMap().size(),
             params,
             adminParams,
             false);
    }

    private TopologyBuilder(TopologyCandidate sourceCandidate,
                            StorageNodePool snPool,
                            int numPartitions,
                            Parameters params,
                            AdminServiceParams adminParams,
                            boolean isInitial) {
        this.sourceCandidate = sourceCandidate;
        this.snPool = snPool;
        this.numPartitions = numPartitions;
        this.params = params;
        logger = LoggerUtils.getLogger(this.getClass(), adminParams);

        /*
         * Validate inputs
         */
        if (snPool.size() < 1) {
            throw new IllegalCommandException(
                "Storage pool " + snPool.getName() + " must not be empty",
                ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }

        /* Check that the SNs in the pool are in the topo. */
        checkPool();

        final Topology sourceTopo = sourceCandidate.getTopology();

        /*
         * The number of partitions must be >= the total capacity of the SN
         * pool divided by the total replication factor of all of the data
         * centers.
         */
        int totalCapacity = 0;
        int totalRF = 0;
        int primaryRF = 0;
        boolean foundPrimaryDC = false;
        final Set<DatacenterId> dcs = new HashSet<>();
        for (final StorageNodeId snId : snPool) {
            final StorageNodeParams snp = params.get(snId);
            totalCapacity += snp.getCapacity();
            final DatacenterId dcId = sourceTopo.get(snId).getDatacenterId();
            if (dcs.add(dcId)) {
                totalRF += sourceTopo.get(dcId).getRepFactor();
                final Datacenter dc = sourceTopo.get(dcId);
                int pRF = dc.getRepFactor();
                if (dc.getDatacenterType().isPrimary() && pRF > 0) {
                    foundPrimaryDC = true;
                    primaryRF += pRF;
                }
            }
        }
        if (!foundPrimaryDC) {
            throw new IllegalCommandException(
                "Storage pool " + snPool.getName() +
                " must contain SNs in a primary zone",
                ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }

        final int minPartitions = totalCapacity / totalRF;
        if (numPartitions < minPartitions) {
            if (isInitial) {
                throw new IllegalCommandException(
                    "The number of partitions requested (" + numPartitions +
                    ") is too small.  There must be at least as many" +
                    " partitions as the total SN capacity in the storage node" +
                    " pool (" + totalCapacity + ") divided by the total" +
                    " replication factor (" + totalRF + "), which is " +
                    minPartitions + ".",
                    ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
            if (numPartitions == 0) {
                throw new IllegalCommandException(
                    "topology create must be run before any other topology " +
                    "commands.",
                    ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
            throw new IllegalCommandException(
                "The number of partitions (" + numPartitions +
                ") cannot be smaller than the total SN capacity in the" +
                " storage node pool (" + totalCapacity +
                ") divided by the total replication factor (" +
                totalRF + "), which is " + minPartitions + ".",
                ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * Verify that the storage directories have been properly configured
         * in that that all have sizes, or none do.
         */
        final Set<StorageNodeId> missingSizes =
                    Rules.validateStorageDirSizes(sourceTopo, snPool, params);
        if (!missingSizes.isEmpty()) {
            throw new CommandFaultException(
                                "Not all storage directories have sizes " +
                                "assigned on storage node(s): " + missingSizes,
                                ErrorMessage.NOSQL_5200,
                                CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * TODO : Add above similar check for rnlogdirs when rnlogdirsize
         * will be exposed.
         */

        totalPrimaryRF = primaryRF;
        arbitersEnabled = ArbiterTopoUtils.useArbiters(sourceTopo);
    }

    /**
     * Correct any non-compliant aspects of the topology.
     */
    public TopologyCandidate rebalance(DatacenterId dcId) {
        /*
         * Ensure that the snpool is a superset of those that already host
         * RNs/ANs. And that all SNs exist and were not previously remove.
         */
        checkSNs(true);

        return rebalance(sourceCandidate.getCopy(), dcId);
    }

    private TopologyCandidate rebalance(final TopologyCandidate startingPoint,
                                        final DatacenterId dcId) {
        return rebalance(startingPoint, dcId,
                         new StoreDescriptor(startingPoint,
                                             params, snPool.getList()));
    }

    /**
     * This flavor used when rebalance is one step for other topology
     * building commands.
     */
    private TopologyCandidate rebalance(final TopologyCandidate startingPoint,
                                        final DatacenterId dcId,
                                        StoreDescriptor currentLayout) {
        if (dcId != null) {
            if (startingPoint.getTopology().get(dcId) == null) {
                throw new IllegalCommandException(dcId +
                                                  " is not a valid zone");
            }
        }
        TopologyCandidate candidate =
            rebalanceRNs(startingPoint, dcId, currentLayout);
        candidate = rebalanceANs(candidate, dcId, currentLayout);

        /*
         * Check if a new candidate was created.
         */
        if (candidate == startingPoint) {
            logger.log(Level.INFO, "{0} has nothing to rebalance",
                       startingPoint.getName());
            return startingPoint;
        }

        /*
         * After rebalance, because the storage directories may have changed,
         * reclaculate the partition distribution.
         */
        distributePartitions(copyStorageDirs(candidate, currentLayout));
        return candidate;
    }


    /**
     * This method corrects RN violations.
     * @param startingCandidate  original candidate
     * @param dcId - if null check all zones, otherwise check only the
     *               specified zone
     * @param currentLayout - current store layout
     * @return  if there are changes a new candidate, otherwise the starting
     *          candidate.
     */
    private TopologyCandidate
        rebalanceRNs(final TopologyCandidate startingCandidate,
                     DatacenterId dcId,
                     StoreDescriptor currentLayout) {

        final Results results =
            Rules.validate(startingCandidate.getTopology(), params, false);

        /* Check for rebalance problems in the validation results. */
        final List<RNProximity> proximity =
            results.find(RNProximity.class,
                         p -> filterByDC(dcId, p.getSNId()));
        final List<OverCapacity> overCap =
            results.find(OverCapacity.class,
                         p -> filterByDC(dcId, p.getSNId()));
        final List<InsufficientRNs> insufficient =
            results.find(InsufficientRNs.class,
                         p -> (dcId == null) || dcId.equals(p.getDCId()));
        final List<ExcessRNs> excessRNsList =
            results.find(ExcessRNs.class,
                         p -> (dcId == null) || dcId.equals(p.getDCId()));

        if (proximity.isEmpty() &&
            overCap.isEmpty() &&
            insufficient.isEmpty() &&
            excessRNsList.isEmpty()) {
            return startingCandidate;
        }

        /*
         * Map out the current layout -- that is, the relationship between all
         * topology components. This is derived from the topology, but fills in
         * the relationships that are implicit but are not stored as fields in
         * the topology.
         */
        TopologyCandidate candidate = startingCandidate.getCopy();
        final Topology candidateTopo = candidate.getTopology();

        /*
         * Fix the violations first.
         */

        /*
         * RNProximity: Find SNs with RNs that are from the same shard and move
         * all but the first one. They should all be fairly equal in cost.
         */
        logger.log(Level.FINE, "{0} has {1} RN proximity problems to fix",
                   new Object[]{candidate.getName(), proximity.size()});

        for (RNProximity r : proximity) {
            final int siblingCount = r.getRNList().size();
            moveRNs(r.getSNId(), candidateTopo, currentLayout,
                    r.getRNList().subList(1, siblingCount));
        }

        /*
         * OverCapacity: move enough RNs off this SN to get it down to
         * budget. Since this decision is done statically, it's hard to predict
         * if any RNs should be preferred as targets over another, so just pick
         * them arbitrarily
         */
        logger.log(Level.FINE, "{0} has {1} over capacity problems to fix",
                   new Object[]{candidate.getName(), overCap.size()});

        for (OverCapacity c : overCap) {
            moveRNs(c.getSNId(), candidateTopo, currentLayout, c.getExcess());
        }

        /*
         * InsufficientRNs: add RNs that are missing
         */
        logger.log(Level.FINE,
                   "{0} has {1} shards with insufficient rep factor",
                   new Object[]{candidate.getName(), insufficient.size()});

        /*
         * Before adding RN check for SN storage directory sizes
         * compatibility with the current RN storage directories
         */
        checkSNStorageDirectorySizeCompatibility();
        for (InsufficientRNs ir : insufficient) {
            addRNs(candidateTopo, ir.getDCId(), ir.getRGId(),
                   currentLayout, ir.getNumNeeded());
        }

        /*
         * ExcessRNs: Remove unneeded RNs
         */
        for (final ExcessRNs excessRNs : excessRNsList) {
            removeRNs(candidateTopo, excessRNs.getDCId(), excessRNs.getRGId(),
                      currentLayout, excessRNs.getNumExcess());
        }

        return candidate;
    }

    /**
     * This method corrects AN violations.
     * @param startingCandidate  original candidate
     * @param dcId - if null check all zones, otherwise check only the
     *               specified zone
     * @param currentLayout - current store layout
     * @return  if there are changes a new candidate, otherwise the starting
     *          candidate.
     */
    private TopologyCandidate
        rebalanceANs(final TopologyCandidate startingCandidate,
                     DatacenterId dcId,
                     StoreDescriptor currentLayout) {

        final Results results =
            Rules.validate(startingCandidate.getTopology(), params, false);

        final List<InsufficientANs> insufficientANs = results.find(
            InsufficientANs.class,
            new RulesProblemFilter<InsufficientANs>() {
                @Override
                public boolean match(final InsufficientANs p) {
                    return ((dcId == null) || dcId.equals(p.getDCId()));
                }
            });

        final List<ExcessANs> excessANs = results.find(
            ExcessANs.class,
            new RulesProblemFilter<ExcessANs>() {
                @Override
                public boolean match(final ExcessANs p) {
                    return filterByDC(dcId, p.getANId());
                }
            });

        final List<ANNotAllowedOnSN> ansnHost = results.find(
            ANNotAllowedOnSN.class,
            new RulesProblemFilter<ANNotAllowedOnSN>() {
                @Override
                public boolean match(final ANNotAllowedOnSN p) {
                    return filterByDC(dcId, p.getANId());
                }
            });

        final List<ANWrongDC> anWrongDCs = results.find(
            ANWrongDC.class,
            new RulesProblemFilter<ANWrongDC>() {
                @Override
                public boolean match(final ANWrongDC p) {
                    return filterByDC(dcId, p.getANId());
                }
            });

        final List<ANProximity> anProximity = results.find(
            ANProximity.class,
            new RulesProblemFilter<ANProximity>() {
                @Override
                public boolean match(final ANProximity p) {
                    return filterByDC(dcId, p.getANId());
                }
            });

        final List<UnevenANDistribution> anDistribution = results.find(
            UnevenANDistribution.class,
            new RulesProblemFilter<UnevenANDistribution>() {
                @Override
                public boolean match(final UnevenANDistribution p) {
                    return filterByDC(dcId, p.getSNId());
                }
            });

        if ( insufficientANs.isEmpty() &&
            excessANs.isEmpty() && anWrongDCs.isEmpty() &&
            ansnHost.isEmpty() && anProximity.isEmpty() &&
            anDistribution.isEmpty()) {
            return startingCandidate;
        }

        /*
         * Map out the current layout -- that is, the relationship between all
         * topology components. This is derived from the topology, but fills in
         * the relationships that are implicit but are not stored as fields in
         * the topology.
         */
        final TopologyCandidate candidate = startingCandidate.getCopy();
        final Topology candidateTopo = candidate.getTopology();


        /*
         * ExcessANs: remove ANs that are not needed
         */
        logger.log(Level.FINE,
                   "{0} has {1} shards with unneeded Arbiter",
                   new Object[]{candidate.getName(), excessANs.size()});

        for (ExcessANs ea : excessANs) {
            ArbNode an = candidateTopo.get(ea.getANId());
            removeAN(candidateTopo, an.getStorageNodeId(), currentLayout,
                    ea.getANId());
        }

        /*
         * InsufficientANs: add ANs that are missing
         */
        logger.log(Level.FINE,
                   "{0} has {1} shards that do not have an Arbiter",
                   new Object[]{candidate.getName(), insufficientANs.size()});

        for (InsufficientANs ia : insufficientANs) {
            addAN(candidateTopo, ia.getDCId(), ia.getRGId(),
                   currentLayout);
        }

        for (ANWrongDC awd : anWrongDCs) {
            ArbNode an = candidateTopo.get(awd.getANId());
            /* check if AN was removed */
            if (an == null) {
                continue;
            }
            moveAN(candidateTopo, an.getStorageNodeId(), currentLayout,
                   awd.getANId(), awd.getTargetDCId());
        }

        for (ANNotAllowedOnSN ana : ansnHost) {
            ArbNode an = candidateTopo.get(ana.getANId());
            /* check if AN was already removed or moved */
            if (an == null ||
                !an.getStorageNodeId().equals(ana.getStorageNodeId())) {
                continue;
            }
            StorageNode sn = candidateTopo.get(an.getStorageNodeId());
            moveAN(candidateTopo, an.getStorageNodeId(), currentLayout,
                   ana.getANId(), sn.getDatacenterId());
        }

        DatacenterId anDC =
            ArbiterTopoUtils.getBestArbiterDC(candidateTopo, params);
        for (ANProximity anp : anProximity) {
            ArbNode an = candidateTopo.get(anp.getANId());
            if (an == null) {
                /* AN no longer in topo */
                continue;
            }
            RepGroup grp = candidateTopo.get(an.getRepGroupId());
            SNDescriptor arbiterSN = findSNForArbiter(currentLayout, grp, anDC);
            if (arbiterSN == null) {
                /* No SN for AN */
                continue;
            }
            moveAN(candidateTopo, anp.getSNId(), currentLayout,
                    an.getResourceId(), anDC);
        }

        if (anDC != null) {
            balanceAN(candidateTopo, currentLayout, anDC);
        }

        return candidate;
    }

    /**
     * Alters the topology to fix AN violations. This is used
     * as part of failover.
     *
     * @param startingPoint initial topology candidate
     * @param offlineZones zones that are offline
     * @return topology candidate
     */
    public TopologyCandidate fixANProblems(final TopologyCandidate startingPoint,
                                           final Set<DatacenterId> offlineZones)
    {
        final Results results =
        Rules.validate(startingPoint.getTopology(), params, false);
        modifiedANs = 0;

        final List<InsufficientANs> insufficientANs =
            results.find(InsufficientANs.class);

        final List<ExcessANs> excessANs = results.find(ExcessANs.class);

        final List<ANWrongDC> anWrongDCs = results.find(ANWrongDC.class);

        if (insufficientANs.isEmpty() &&
            excessANs.isEmpty() && anWrongDCs.isEmpty()) {
            return startingPoint;
        }

        final StoreDescriptor currentLayout =
            new StoreDescriptor(startingPoint, params, snPool.getList());
        final TopologyCandidate candidate = startingPoint.getCopy();
        final Topology candidateTopo = candidate.getTopology();

        for (ExcessANs ea : excessANs) {
            ArbNode an = candidateTopo.get(ea.getANId());
            removeAN(candidateTopo, an.getStorageNodeId(), currentLayout,
                     ea.getANId());
            modifiedANs++;
        }

        for (InsufficientANs ia : insufficientANs) {
            boolean added = addAN(candidateTopo, ia.getDCId(), ia.getRGId(),
                   currentLayout);
            if (added) {
                modifiedANs++;
            }
        }

        for (ANWrongDC awd : anWrongDCs) {
            ArbNode an = candidateTopo.get(awd.getANId());
            /* check if AN was removed */
            if (an == null) {
                continue;
            }
            if (offlineZones.contains(awd.getSourceDCId())) {

                moveAN(candidateTopo, an.getStorageNodeId(), currentLayout,
                       awd.getANId(), awd.getTargetDCId());
                modifiedANs++;
            }
        }

        return candidate;
    }

    public int getModifiedANs() {
        return modifiedANs;
    }

    /**
     * Contract an existing store. The steps to contracted the store is are as
     * follows:
     * 1. Combine shards and remove shards according to the number of SNs in
     * StorageNodePool.
     * 2. Move RNs from SNs out of StorageNodePool to SNs in StorageNodePool.
     * 3. If any one of the steps fails, the topology candidate will back to
     * the original one.
     * 4. The final step is to rebalance the contracted topology to make sure
     * the topology is balance.
     */
    public TopologyCandidate contract() {

        /*
         * Ensure that the snpool is a subset of those that already host
         * RNs/ANs. And that all SNs exist and were not previously remove.
         */
        checkSNs(false);

        final TopologyCandidate candidate = sourceCandidate.getCopy();
        final Topology startingTopo = candidate.getTopology();

        try {
            /*
             * Calculate the maximum number of shards this store can host,
             * looking only at the physical resources available, and ignoring
             * any existing, assigned RNs.
             */
            final int currentShards = startingTopo.getRepGroupMap().size();
            final EmptyLayout ideal =
                                new EmptyLayout(candidate, params, snPool);
            final int maxShards = ideal.getMaxShards();

            /*
             * Don't permit a reduction in the number of zones.
             */
            final int newNumDCs = ideal.getDCDesc().size();
            final int oldNumDCs = startingTopo.getDatacenterMap().size();
            if (newNumDCs < oldNumDCs) {
                throw new IllegalCommandException
                    ("Insufficient storage nodes to support current zones.",
                     ErrorMessage.NOSQL_5200,
                     CommandResult.NO_CLEANUP_JOBS);
            }

            /*
             * Don't permit the creation of an empty topology. It would have
             * no partitions.
             */
            if (maxShards == 0) {
                throw new IllegalCommandException
                    ("Available storage nodes would result in a store with " +
                     "no shards.",
                     ErrorMessage.NOSQL_5200,
                     CommandResult.NO_CLEANUP_JOBS);
            }

            if (maxShards >= currentShards) {
                logger.info("Couldn't contract topology. The number of shards "+
                            "was not reduced.\nShard calculation " +
                            "based on smallest zone: " +
                            ideal.showDatacenters());
                return assignStorageDirectories(rebalance(candidate, null));
            }

            /*
             * Check whether the to-be-contracted SNs host Admins or not.
             * If any to-be-contracted SNs host Admins, the operation fails.
             */
            List<StorageNodeId> adminSNs =
                    getContractedSNsHostAdmin(candidate);
            if (!adminSNs.isEmpty()){
                throw new IllegalCommandException
                ("Cannot contract the topology. One or more of the " +
                 "storage nodes targeted for removal manages an Admin " +
                 "service. The Admin serivce(s) on " + adminSNs +
                 " must first be moved or removed  manually.",
                 ErrorMessage.NOSQL_5200,
                 CommandResult.NO_CLEANUP_JOBS);
            }

            /* Calculate the number of to-be-removed shards */
            int numRemovedShards = currentShards - maxShards;

            /*
             * Identify to-be-removed shards, redistribute partitions and
             * remove to-be-removed shards
             */
            identifyRemovedShards(candidate, numRemovedShards);
            distributePartitions(candidate);
            removeShards(candidate);

            final StoreDescriptor currentLayout =
                new StoreDescriptor(candidate, params,
                                    startingTopo.getSortedStorageNodeIds());
            /* Move RNs under SNs not in StorageNodePool to other SNs */
            if (!moveRNsForContractedTopology(candidate, currentLayout)) {
                /*
                 * a failure during layout could have left the candidate's
                 * topology in an interim state. Reset it to the original,
                 * but retain its audit log and other useful information.
                 */
               candidate.resetTopology(startingTopo.getCopy());

               throw new IllegalCommandException
               ("Cannot find enough SNs to host moved RNs and ANs for " +
                candidate.getName(),
                ErrorMessage.NOSQL_5200,
                CommandResult.NO_CLEANUP_JOBS);
            }

            /* Return contracted topology candidate and improved parameters */
            return copyStorageDirs(rebalance(candidate, null, currentLayout),
                                   currentLayout);
        } catch (RuntimeException e) {
            logger.log(Level.INFO,
                       "Topology contract failed due to {0}\n{1}",
                       new Object[] { e, candidate.showAudit()});
            throw e;
        }
    }

    /**
     * Remove failed shard from an existing store. The steps to remove failed
     * shard from the store are as follows:
     * 1. Remove shard according to the shardId passed in remove-shard command.
     * 2. Remove SNs from the StorageNodePool if they were only hosting RNs for
     * the removed shard.
     * 3. If any one of the steps fails, the topology candidate will revert
     * back to the original one.
     * 4. The final step is to rebalance the topology after remove-shard to
     * make sure the topology is balance.
     * @param failedShard the failed shard Id.
     */
    public TopologyCandidate removeFailedShard(RepGroupId failedShard) {

        /*
         * Ensure that the snpool is a subset of those that already host
         * RNs/ANs. And that all SNs exist and were not previously remove.
         */
        checkSNs(false);

        final TopologyCandidate candidate = sourceCandidate.getCopy();
        final Topology startingTopo = candidate.getTopology();

        try {
            final int currentShards = startingTopo.getRepGroupMap().size();

            /*
             * Don't permit the creation of an empty topology. It would have
             * no partitions.
             */
            if ((currentShards-1) <= 0) {
                throw new IllegalCommandException
                    ("Removing shard operation would result in a store with " +
                     "no shards.",
                     ErrorMessage.NOSQL_5200,
                     CommandResult.NO_CLEANUP_JOBS);
            }

            /*
             * Identify to-be-removed shards, redistribute partitions and
             * remove to-be-removed shards.
             *
             * Add failedShard to removeRgIds. We are assuming only one
             * shard is specified to be removed.
             */
            removedRgIds.add(failedShard);
            distributePartitions(candidate);
            removeShards(candidate);

            /* Return topology candidate after remove-shard and improved
             * parameters. Although remove-shard might not need rebalance
             * operation */
            return rebalance(candidate, null);
        } catch (RuntimeException e) {
            logger.log(Level.INFO,
                       "Topology remove-shard failed due to {0}\n{1}",
                       new Object[] { e, candidate.showAudit()});
            throw e;
        }
    }

    /**
     * Get the to-be-contracted SNs hosting Admins. The to-be-contracted SNs
     * hosting RNs or ANs in the source topology and do not exist in the snpool.
     * The steps are as follows:
     * 1. Find all SNs hosting Admins
     * 2. Remove the SNs in snpool from the list of SNs hosting Admins
     * 3. Find out the SNs not containing ANs or RNs in the source topology.
     * 4. Remove the SNs not containing ANs or RNs from the list of SNs
     * hosting Admins
     * @return the list of to-be-contracted SNs which host Admins.
     */
    private List<StorageNodeId> getContractedSNsHostAdmin
                                    (TopologyCandidate candidate) {
        Topology topo = candidate.getTopology();

        /* Step 1: List of SNs containing Admin */
        final List<StorageNodeId> snAdminList = new ArrayList<>();

        /* Iterate all Admins and find the SNs hosting Admins */
        for (AdminParams ap : params.getAdminParams()) {
            final StorageNode sn =
                    topo.getStorageNodeMap().get(ap.getStorageNodeId());

            /* Admin parameters pointing to a non-existent SN */
            if (sn == null) {
                continue;
            }

            snAdminList.add(sn.getStorageNodeId());
        }

        /*
         * Step 2: Remove all SNs in the snpool, the left SNs might be the
         * to-be-contracted SNs
         */
        snAdminList.removeAll(snPool.getList());

        /* Step 3: Find the SNs not containing any RNs or ANs in the topology */
        List<StorageNodeId> emptySNs = new ArrayList<>();
        emptySNs.addAll(topo.getStorageNodeIds());

        /* Remove SNs hosting RNs from empty SNs list */
        for (RepNode rn : topo.getSortedRepNodes()) {
            emptySNs.remove(rn.getStorageNodeId());
        }

        /* Remove SNs hosting ANs from empty SNs list */
        for (ArbNode an : topo.getSortedArbNodes()) {
            emptySNs.remove(an.getStorageNodeId());
        }

        /* Step 4: Remove the SNs not containing ANs or RNs */
        snAdminList.removeAll(emptySNs);

        return snAdminList;
    }

    /**
     * Identify which shards will be removed from topology base on the removed
     * shards number. A to-be-removed shard should be the shard having the
     * highest number. For example, there are 5 shards, and 2 shards will be
     * removed. And the to-be-removed shards should be shard4 and shard5.
     */
    private void identifyRemovedShards(TopologyCandidate candidate,
                                       int numRemovedShards) {
        final Topology candidateTopo = candidate.getTopology();

        /*
         * Reverse the sorted list. And then identify the to-be-removed shards
         * from the first one of the list.
         */
        List<RepGroupId> list = candidateTopo.getSortedRepGroupIds();
        Collections.reverse(list);
        Iterator<RepGroupId> iter = list.iterator();

        while (iter.hasNext() && numRemovedShards > 0) {
            removedRgIds.add(iter.next());
            numRemovedShards--;
        }
    }

    /**
     * Remove to-be-removed shards from a topology candidate
     */
    private void removeShards(TopologyCandidate candidate) {
        final Topology candidateTopo = candidate.getTopology();
        logger.log(Level.FINE, "Removing {0} shard(s) from {1}",
                   new Object[]{removedRgIds.size(), candidate.getName()});

        for (RepGroupId rgId : removedRgIds) {
            /*
             * Remove the nodes from the topology. This operation does not need
             * a sorted set of IDs, but the sorted methods create copies vs.
             * returning a iterator to the original map. This avoids CME when
             * removing the ID from the topology.
             */
            for (ArbNodeId anId : candidateTopo.getSortedArbNodeIds(rgId)) {
                candidateTopo.remove(anId);
            }
            for (RepNodeId rnId : candidateTopo.getSortedRepNodeIds(rgId)) {
                logger.log(Level.FINE, "Removing {0} from {1}",
                           new Object[]{rnId, rgId});
                candidateTopo.remove(rnId);
                candidate.removeDirectoryAssignment(rnId);
                candidate.removeRNLogDirectoryAssignment(rnId);
            }
            candidateTopo.remove(rgId);
        }
        /*
         * Now that the groups have been removed from the candidate and topo
         * clear the to-be-removed list to avoid being counted twice.
         */
        removedRgIds.clear();
    }

    /**
     * Move RNs for a specified contracted topology base on SNs in
     * StorageNodePool
     * @return true when move successfully or return false.
     */
    private boolean
        moveRNsForContractedTopology(TopologyCandidate candidate,
                                     StoreDescriptor currentLayout) {
        /* Map SNs not in StorageNodePool and the RNs under them */
        Map<StorageNodeId, List<RepNodeId>> removedRNsforSNs = new HashMap<>();

        /* Map SNs not in StorageNodePool and the ANs under them */
        Map<StorageNodeId, List<ArbNodeId>> removedANsforSNs = new HashMap<>();
        Topology topo = candidate.getTopology();

        /*
         * Loop all SNs and find out SNs not in StorageNodePool and their RNs
         * and ANs
         */
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            /* Filter out SNs in the topology which are in StorageNodePool */
            if (snPool.contains(snId)) {
                continue;
            }

            /* Get all RNs and ANs under a SN not in StorageNodePool */
            final SNDescriptor owningSND = currentLayout.getSND(snId, topo);
            assert !owningSND.inPool;
            List<RepNodeId> RNsList = removedRNsforSNs.get(snId);
            if (RNsList == null) {
                RNsList = new ArrayList<>();
                removedRNsforSNs.put(snId, RNsList);
            }
            RNsList.addAll(owningSND.getRNs());

            List<ArbNodeId> ANsList = removedANsforSNs.get(snId);
            if (ANsList == null) {
                ANsList = new ArrayList<>();
                removedANsforSNs.put(snId, ANsList);
            }
            ANsList.addAll(owningSND.getARBs());
        }

        /* Move RNs of SNs not in StorageNodePool to other SNs */
        for (Map.Entry<StorageNodeId, List<RepNodeId>> entry :
                                                removedRNsforSNs.entrySet()) {
            /* return false when can not find enough SNs to host RNs */
            if (!moveRNs(entry.getKey(), topo, currentLayout,
                         entry.getValue())) {
                return false;
            }
        }

        topo = candidate.getTopology();
        final DatacenterId dcId = ArbiterTopoUtils.getArbiterDC(topo, params);

        /* Move ANs of SNs not in StorageNodePool to other SNs */
        for (Map.Entry<StorageNodeId, List<ArbNodeId>> entry :
                                                removedANsforSNs.entrySet()) {
            for (ArbNodeId anId : entry.getValue()) {
                /* return false when can not find enough SNs to host ANs */
                if (!moveAN(topo, entry.getKey(), currentLayout, anId, dcId)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Build an initial store, or redistribute an existing store. Rebalance is
     * executed first, so that the starting topology is as clean as possible
     * before we try a redistribute.  Note that the incoming topology may
     * nevertheless have rule violations, and the resulting topology may also
     * have rule violations. The calling layer must be sure to warn about and
     * display any violations found in the candidate.
     *
     * If the builder is unable to improve the topology, it will return the
     * original topology.
     */
    public TopologyCandidate build() {

        /*
         * Ensure that the snpool is a superset of those that already host
         * RNs/ANs. And that all SNs exist and were not previously remove.
         */
        checkSNs(true);

        logger.log(Level.FINE, "Build {0} using {1}, numPartitions={2}",
                  new Object[]{sourceCandidate.getName(), snPool.getName(),
                               numPartitions});

        /* Make sure the starting point has been rebalanced. */
        final TopologyCandidate startingCandidate =
                                rebalance(sourceCandidate.getCopy(), null);
        final Topology startingTopo = startingCandidate.getTopology();

        TopologyCandidate candidate = null;

        try {

            /*
             * Calculate the maximum number of shards this store can host,
             * looking only at the physical resources available, and ignoring
             * any existing, assigned RNs.
             */
            final int currentShards = startingTopo.getRepGroupMap().size();
            final EmptyLayout ideal =
                new EmptyLayout(startingCandidate, params, snPool);
            int maxShards = ideal.getMaxShards();

            /*
             * Don't permit the creation of an empty, initial topology. If
             * deployed, it would have no partitions, and the number of
             * partitions can't be changed after the initial deployment.
             */
            if ((currentShards == 0) && (maxShards == 0)) {
                throw new IllegalCommandException
                    ("It is not possible to create any shards for this " +
                     "initial topology with the available set of storage " +
                     "nodes. Please increase the number or capacity of " +
                     "storage nodes", ErrorMessage.NOSQL_5200,
                     CommandResult.NO_CLEANUP_JOBS);
            }

            if (maxShards > numPartitions) {
                logger.log(Level.INFO,
                           "Insufficient number of partitions ({0}) to "+
                           "support the optimal number of shards ({1}).",
                           new Object[]{numPartitions, maxShards});
                maxShards = numPartitions;
            }

            /* Can we add any shards? */
            if (maxShards <= currentShards) {
                logger.info("Couldn't improve topology. Store can only " +
                            "support " +  maxShards  + " shards." +
                            "\nShard calculation based on smallest " +
                            "zone: " +
                            ideal.showDatacenters());

                /*
                 * Even if we couldn't add shards, see if we need any
                 * redistribution.
                 */
                distributePartitions(
                                   assignStorageDirectories(startingCandidate));
                return startingCandidate;
            }

            /*
             * The maximum number of shards may or may not be achievable.
             * Existing RNs may be too costly to move, or this redistribution
             * implementation may be too simplistic to do all the required
             * moves. Repeat the attempts until each datacenter has been mapped
             * for more shards than previously existed. If not possible, give
             * up. For example, suppose the existing store has 20 shards, and
             * the first calculations thinks we can get to 25 shards. Try to
             * map each datacenter with first 25 shards, then 24, etc, until
             * we've arrived at a number that is > 20, and which all
             * datacenters can support.
             *
             * TODO: handle the case where even the current number of shards is
             * not ideal, yet partitions still need to be moved.
             */
            boolean success = false;
            for (int currentGoal = maxShards; currentGoal > currentShards;
                 currentGoal--) {

                /*
                 * Create a starting candidate, and relationship descriptor
                 * which represent all the components in the current topology..
                 */
                candidate = startingCandidate.getCopy();
                final StoreDescriptor currentLayout =
                        new StoreDescriptor(candidate, params, snPool.getList());

                if (layoutShards(candidate, currentLayout, currentGoal)) {
                    success = true;
                    break;
                }
            }

            if (!success) {
                /*
                 * a failure during layout could have left the candidate's
                 * topology in an interim state. Reset it to the original,
                 * but retain its audit log and other useful information.
                 */
                if (candidate != null) {
                    candidate.resetTopology(startingTopo.getCopy());
                }
            }
            return candidate;
        } catch (RuntimeException e) {
            if (candidate == null) {
                logger.log(Level.INFO,
                           "Topology build failed due to " + e);

            } else {
                logger.log(Level.INFO,
                           "Topology build failed due to {0}\n{1}",
                           new Object[] { e, candidate.showAudit()});
            }
            throw e;
        }
    }

    /**
     * Change the repfactor on an existing datacenter. This will cause shards
     * to become non-compliant because they do not have enough SNs, so do a
     * rebalance so enough SNs are added.
     */
    public TopologyCandidate changeRepfactor(int newRepFactor,
                                             DatacenterId dcId) {
        /*
         * Ensure that the snpool is a superset of those that already host
         * RNs/ANs. And that all SNs exist and were not previously remove.
         */
        checkSNs(true);

        final Datacenter dc = sourceCandidate.getTopology().get(dcId);
        if (dc == null) {
            throw new IllegalCommandException(dcId +
                                              " is not a valid zone");
        }

        checkNewRepFactor(dc, newRepFactor);

        /* Update the replication factor */
        final TopologyCandidate startingPoint =sourceCandidate.getCopy();
        if (dc.getRepFactor() != newRepFactor) {
            startingPoint.getTopology().update(
                dcId,
                Datacenter.newInstance(dc.getName(), newRepFactor,
                                       dc.getDatacenterType(),
                                       dc.getAllowArbiters(),
                                       dc.getMasterAffinity()));
        }

        /* Add or remove RNs to fulfill the desired replication factor */
        return rebalance(startingPoint, dcId);
    }

    /**
     * Checks a new zone replication factor.
     *
     * @param dc the datacenter
     * @param rf the new rep factor
     * @return true if the change is permitted, false if there is no change
     * @throws IllegalCommandException if the change is not permitted because
     * of an attempt to reduce the RF of a primary zone, or if the rep factor
     * is out of bounds
     */
    public static boolean checkNewRepFactor(Datacenter dc, int rf) {
        if (dc.getRepFactor() == rf) {
            /* Nothing to do */
            return false;
        }
        Rules.validateChangeReplicationFactor(rf, dc);
        return true;
    }

    /**
     * Move the specified partition to the specified shard. Throws
     * IllegalCommandException if the partition does not exist, the
     * partition is already in the shard, or the shard does not exist.
     *
     * @param partId partition
     * @param rgId target shard
     * @return modified topology candidate
     */
    public TopologyCandidate movePartition(PartitionId partId, RepGroupId rgId) {
        final Topology topo = sourceCandidate.getTopology().getCopy();
        Partition p = topo.get(partId);
        if (p == null) {
            throw new IllegalCommandException(partId.getPartitionId() +
                                              " does not exist");
        }
        if (p.getRepGroupId().equals(rgId)) {
            throw new IllegalCommandException(partId.getPartitionId() +
                                              " is already in " + rgId);
        }
        if (topo.get(rgId) == null) {
            throw new IllegalCommandException(rgId + " does not exist");
        }
        topo.updatePartition(partId, rgId);
        return new TopologyCandidate(sourceCandidate.getName(), topo);
    }

    /**
     * Move the specified RN off its current SN. Meant as a limited way for
     * the user to manually modify the topology. The prototypical use case is
     * that there is a hardware fault with that RN, but not with the whole
     * SN (which pretty much means the storage directory), and the user would
     * like to move the RN away before attempting repairs.
     *
     * If a SN is specified, the method will attempt to move to that specific
     * node. This will be a hidden option in R2; the public option will only
     * permit moving the RN off an SN, onto some SN chosen by the
     * TopologyBuilder.
     */
    public TopologyCandidate relocateRN(RepNodeId rnId,
                                        StorageNodeId proposedSNId) {

        final Topology topo = sourceCandidate.getTopology().getCopy();
        final RepNode rn = topo.get(rnId);
        if (rn == null) {
            throw new IllegalCommandException(rnId + " does not exist");
        }

        final StorageNodeId oldSNId = rn.getStorageNodeId();
        final StoreDescriptor currentLayout =
                new StoreDescriptor(sourceCandidate.getCopy(),
                                    params, snPool.getList());
        final SNDescriptor owningSND = currentLayout.getSND(oldSNId, topo);
        final DatacenterId dcId = currentLayout.getOwningDCId(oldSNId, topo);

        final List<SNDescriptor> possibleSNDs;
        if (proposedSNId == null) {
            possibleSNDs = currentLayout.getAllSNDs(dcId);
        } else {
            final StorageNode proposedSN = topo.get(proposedSNId);
            if (proposedSN == null) {
                throw new IllegalCommandException("Proposed target SN " +
                                                  proposedSNId +
                                                  " does not exist");
            }

            if (!dcId.equals(proposedSN.getDatacenterId())) {
                throw new IllegalCommandException
                    ("Can't move " + rnId + " to " + proposedSN +
                     " because it is in a different zone");
            }
            possibleSNDs = new ArrayList<>();
            possibleSNDs.add(currentLayout.getSND(proposedSNId, topo));
        }

        for (SNDescriptor snd : possibleSNDs) {
            if (snd.getId().equals(oldSNId)) {
                continue;
            }

            logger.log(Level.FINEST, "Trying to move {0} to {1}",
                       new Object[]{rn, snd});
            final StorageDirectory sd = snd.findStorageDir(rnId);
            final LogDirectory rld = snd.findRNLogDir();
            if (sd != null) {
                /* Move the RN in the descriptions. */
                snd.claim(rnId, owningSND, sd, rld);
                changeSNForRN(topo, rnId, snd.getId());
                break;
            }
        }

        /**
         * Note that after a relocation the partition distribution may be
         * incorrect. We do not do a distributePartitions() here because
         * relocate is generally used to fix or work around failures. In this
         * case we want to change as little as possible.
         */
        return copyStorageDirs(new TopologyCandidate(sourceCandidate.getName(),
                                                     topo),
                               currentLayout);
    }

    /**
     * Choose the SN in the input list to host an Arbiter.
     * The SN with the lowest number of hosted AN is selected.
     * If there are multiple SN with the same number of AN's,
     * zero capacity SN are given priority over non-zero.
     * The SN with the higher capacity is given priority if
     * both SN have non-zero capacity.
     *
     * @param sndList SNs without RNs from same shard.
     * @return SN that is deemed best to host Arbiter
     */
    private SNDescriptor findSNForArbiter(List<SNDescriptor> sndList) {

        SNDescriptor bestSn = sndList.get(0);
        for (int i = 1; i < sndList.size(); i++) {
            if (compareForArbiter(bestSn, sndList.get(i)) > 0) {
                bestSn = sndList.get(i);
            }
        }
        return bestSn;
    }

    /**
     * Compare two SN's with respect to hosting
     * Arbiters.
     *
     * @param sn1
     * @param sn2
     * @return -1 if sn1 is better, 1 if sn2 is better
     */
    private int compareForArbiter(SNDescriptor sn1, SNDescriptor sn2) {

        /* Make determination based on number of hosted Arbiters. */
        if (sn1.getARBs().size() !=  sn2.getARBs().size()) {
            return Integer.signum(sn1.getARBs().size() - sn2.getARBs().size());
        }

        /* Make determination based on capacity. */
        int sn1Cap = sn1.getCapacity();
        int sn2Cap = sn2.getCapacity();

        /*
         * Capacity zero SN's have priority over non-zero capacity SNs.
         */
        if (sn1Cap != sn2Cap) {
            if (sn1Cap == 0 && sn2Cap > 0) {
                return -1;
            }

            if (sn2Cap == 0 && sn1Cap > 0) {
                return 1;
            }
            return Integer.signum(sn2Cap - sn1Cap);
        }

        /* Capacities are equal so use lower snId */
        int sn1Id =  sn1.getId().getStorageNodeId();
        int sn2Id =  sn2.getId().getStorageNodeId();
        return Integer.signum(sn1Id - sn2Id);
    }

    /**
     * Find the best SN to place the arbiter. Involves 2 passes.
     * The first pass finds SN's that do not host RN's in the same shard.
     * From this list of SNs we then find the best SN in the second pass.
     * If the list returned in first pass is empty, we return null which
     * implies there is no SN available to place the arbiter.
     *
     * @return arbiterSN - the SN to place the arbiter. If an SN is not
     * found return null.
     */
    private SNDescriptor findSNForArbiter(StoreDescriptor currentLayout,
                                          RepGroup grp,
                                          DatacenterId arbiterDCId) {

        SNDescriptor arbiterSN = null;
        List<SNDescriptor> allSNDs = currentLayout.getAllSNDs(arbiterDCId);
        List<SNDescriptor> notInShardSNDs = new ArrayList<>();
        Set<StorageNodeId> shardSNs = new HashSet<>();

        /* Find all SN's in this shard. */
        for (final RepNode rn : grp.getRepNodes()) {
            shardSNs.add(rn.getStorageNodeId());
        }

        /* Find possible SN's to host this shards Arbiter. */
        for (SNDescriptor eachSND : allSNDs) {
            /* Filter out the SNs not in StorageNodePool */
            if(!snPool.contains(eachSND.getId())) {
                continue;
            }
            if (eachSND.getAllowArbiters() &&
                !shardSNs.contains(eachSND.getId())) {
                /* Add candidate */
                notInShardSNDs.add(eachSND);
            }
        }

        if (!notInShardSNDs.isEmpty()) {
            arbiterSN = findSNForArbiter(getSortedSNs(notInShardSNDs));
        }

        return arbiterSN;
    }

    /**
     * Assign Arbiters to SNs. Update both the relationship descriptors and
     * the candidate topology. This method assumes that Arbiters are to be
     * used and that there is at least one DC that is configured to use
     * Arbiters.
     * The number of SNs in the Arbiter DC must be at least RF + 1
     * in order to have chance to layout the Arbiters. If not
     * throw IllegalCommandException.
     *
     * @return true if successful
     * @throws IllegalCommandException if the Arbiter DC does not have
     *         at enough SN's to layout one shard.
     */
    private boolean layoutArbiters(TopologyCandidate candidate,
                                   StoreDescriptor currentLayout) {

        Topology topo = candidate.getTopology();
        final DatacenterId dcId = ArbiterTopoUtils.getArbiterDC(topo, params);

        /*
         * Check for ANProximity violations that could have occurred
         * after RN layout.
         */
        final Results results = Rules.validate(topo, params, false);
        final List<ANProximity> anProximity = results.find(
                ANProximity.class,
                new RulesProblemFilter<ANProximity>() {
                    @Override
                    public boolean match(final ANProximity p) {
                        return filterByDC(dcId, p.getANId());
                    }
                });

        for (ANProximity anp : anProximity) {
            ArbNode an = topo.get(anp.getANId());
            RepGroup grp = topo.get(an.getRepGroupId());
            SNDescriptor arbiterSN = findSNForArbiter(currentLayout, grp, dcId);
            if (arbiterSN == null) {
                /* No SN to move AN */
                continue;
            }
            moveAN(topo, arbiterSN.getId(), currentLayout,
                   an.getResourceId(), dcId);
        }

        /*
         * Note that this method assumes that it is called with at
         * least one DC configured to support Arbiters so dcId
         * will not be null.
         */
        candidate.log("Target arbiter datacenter: " + dcId);

        final Topology sourceTopo = sourceCandidate.getTopology();
        final int usableSNs =
               ArbiterTopoUtils.getNumUsableSNs(sourceTopo, params, dcId);
        Datacenter arbDc = topo.get(dcId);
        if (usableSNs < arbDc.getRepFactor() + 1) {
            /* If there is not at least one more SN than the Arbiter
             * DC repfactor or if there is not atleast one SN that can host
             * arbiters, we cannot successfully layout a shard.
             */
            int needsSN = arbDc.getRepFactor() + 1;
            throw new IllegalCommandException(
                "It is not possible to create any shards with " +
                "Arbiters for this initial topology. The minimum of " +
                needsSN +
                " SNs are required in the arbiter " + dcId +
                " zone. The current number of SN's that may be used to host " +
                "Arbiters is " + usableSNs + ". " +
                "Please increase the number of Arbiter hosting storage nodes.");
        }

        /*
         * Sort the shards so that the arbiters are placed in an orderly
         * manner. The RN layout is done by shard,SN order. The layout of
         * AN's is performed in reverse shard order, SN order. This enables
         * a little better assignment layout since the shard,SN assignment is
         * roughly ordered from low to high [#24544].
         */
        List<RepGroupId> rgIds = topo.getSortedRepGroupIds();
        for (int i = rgIds.size() - 1; i >= 0; i--) {
            RepGroupId rgId = rgIds.get(i);
            RepGroup grp = topo.get(rgId);

            if (sourceCandidate.getTopology().get(rgId) != null) {
                /*
                * Don't add arbiters to pre-existing shards
                */
                continue;
            }
            SNDescriptor arbiterSN = findSNForArbiter(currentLayout, grp,
                                                      dcId);
            if (arbiterSN == null){
                Datacenter arbDC = sourceTopo.get(dcId);
                candidate.log(
                    "It is not possible to create any shards with " +
                    "Arbiters for this initial topology. " +
                    "Please increase the number of storage nodes in " +
                    arbDC.getName() + " Datacenter.");
                return false;
            }


            addOneARB(topo, arbiterSN, grp.getResourceId());
            candidate.log("Added arbiter to shard " + rgId);
        }

        /* Try to better balance the AN distribution. */
        balanceAN(candidate.getTopology(), currentLayout, dcId);
        return true;
    }

    /**
     * Return true if this snId is in this datacenter.
     * if filterDC == null, return true.
     * if filterDC != null, return true if snId is in this datacenter
     */
    private boolean filterByDC(DatacenterId filterDCId, StorageNodeId snId) {
        if (filterDCId == null) {
            return true;
        }

        return (sourceCandidate.getTopology().get(snId).getDatacenterId().equals(filterDCId));
    }

    /**
     * Return true if this anId is in this datacenter.
     * if filterDC == null, return true.
     * if filterDC != null, return true if anId is in this datacenter
     */
    private boolean filterByDC(DatacenterId filterDCId, ArbNodeId anId) {
        if (filterDCId == null) {
            return true;
        }
        final Topology sourceTopo = sourceCandidate.getTopology();
        final StorageNodeId snId = sourceTopo.get(anId).getStorageNodeId();
        return (sourceTopo.get(snId).getDatacenterId().equals(filterDCId));
    }

    /**
     * Method that return the notes (Information regarding topology command
     * usages, warnings or any information that might help user in topology
     * building and deployment) while topology building.
     */
    public Set<String> getNotes(){
        return notes;
    }

    /**
     * Checks that the snpool provided for the new topology contains nodes know
     * to the parameters and the topology.
     */
    private void checkPool() {
        final Topology sourceTopo = sourceCandidate.getTopology();
        for (StorageNodeId snId : snPool.getList()) {
            if (params.get(snId) == null) {
                throw new IllegalCommandException
                    ("Storage Node " + snId + " does not exist. " +
                     "Please remove " + "it from " + snPool.getName(),
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }

            if (sourceTopo.get(snId) == null) {
                throw new IllegalCommandException
                    ("Topology candidate " + sourceCandidate.getName() +
                     " does not know about " + snId +
                     " which is a member of storage node pool " +
                     snPool.getName() +
                     ". Please use a different storage node pool or " +
                     "re-clone your candidate using the command " +
                     "topology clone -current -name <candidateName>",
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
        }
    }

    /**
     * Check that the snpool provided for the new topology is a superset of
     * those that already host RNs/ANs, and that all SNs exist, or the new
     * topology is a subset of those SNs.
     *
     * @param isSuperSetCheck is true, the method is to check whether snpool is
     * the superset of all SNs exist; or it to check whether snpool is the
     * subset of those SNs.
     */
    private void checkSNs(boolean isSuperSetCheck) {
        final Set<StorageNodeId> inTopo = new HashSet<>();
        final Topology sourceTopo = sourceCandidate.getTopology();
        for (RepNode rn : sourceTopo.getSortedRepNodes()) {
            inTopo.add(rn.getStorageNodeId());
        }

        for (ArbNode an : sourceTopo.getSortedArbNodes()) {
            inTopo.add(an.getStorageNodeId());
        }
        final Set<StorageNodeId> inPool = new HashSet<>(snPool.getList());
        final Set<StorageNodeId> missing = new HashSet<>(inTopo);
        final Set<StorageNodeId> extra = new HashSet<>(inPool);

        missing.removeAll(inPool);
        extra.removeAll(inTopo);
        if (isSuperSetCheck) {
            if (missing.size() > 0) {
                throw new IllegalCommandException
                     ("The storage pool provided for topology candidate " +
                      sourceCandidate.getName() +
                      " must contain the following SNs " +
                      "which are already in use in the current topology: " +
                      missing, ErrorMessage.NOSQL_5200,
                      CommandResult.NO_CLEANUP_JOBS);
            }
        } else {
            if (extra.size() > 0) {
                throw new IllegalCommandException
                 ("The storage pool provided for topology candidate " +
                  sourceCandidate.getName() +
                  " should not contain the following SNs " +
                  "which are not in the current topology: " + extra,
                  ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
        }
    }

    /**
     * It is the compatibility check of the SN storage directory sizes with
     * the current RN sizes.The reason is that when we generate a new
     * topology, all the storage directories that have a smaller size than
     * all the current storage directories will be skipped when allocating new
     * RNs.
     * The minimum, however, is computed with all the current RNs. So this check
     * ensures the proper message is displayed as a note when we execute a
     * topology change-repfactor, rebalance or redistribute command with
     * smaller storage directory sizes.
     * This check will display a note for the client in case of incompatible
     * storage directory sizes and the execution of the topology command will
     * not be stopped.
     * It may not cover all the scenario in which the storage directories may
     * be skipped because of incompatible storage directory sizes.
     */
    private void checkSNStorageDirectorySizeCompatibility() {
        final Map<StorageNodeId, Set<String>> inUseMountPoints =
            new HashMap<>();
        long smallestExistingSize = Long.MAX_VALUE;
        for (final RepNodeParams rnParams : params.getRepNodeParams()) {
            final String path = rnParams.getStorageDirectoryPath();
            if (path == null) {
                continue;
            }
            inUseMountPoints.computeIfAbsent(rnParams.getStorageNodeId(),
                                             snId -> new HashSet<>()).add(path);
            final long size = rnParams.getStorageDirectorySize();
            if ((size >= 0) && (size < smallestExistingSize)) {
                smallestExistingSize = size;
            }
        }
        final Map<StorageNodeId, Map<String, Long>> smallerSizes =
            new TreeMap<>();
        for (final StorageNodeParams snParams :
            params.getStorageNodeParams()) {
            if ((snParams == null) || (snParams.getStorageDirMap() == null)) {
                continue;
            }
            final StorageNodeId snId = snParams.getStorageNodeId();
            for (final Parameter storageDirParam :
                snParams.getStorageDirMap()) {
                final String path = storageDirParam.getName();
                if (!inUseMountPoints.containsKey(snId) ||
                    !inUseMountPoints.get(snId).contains(path)) {
                    final long size = SizeParameter.getSize(storageDirParam);
                    if (size < smallestExistingSize) {
                        smallerSizes.computeIfAbsent(snId,
                                                     ignore -> new TreeMap<>())
                                                     .put(path, size);
                    }
                }
            }
        }
        if (!smallerSizes.isEmpty()) {
            String msg = "Some new SN storage directory sizes are smaller" +
                " than the smallest existing storage directory size, which" +
                " may prevent new SNs from being used in the new topology." +
                " The smallest existing SN storage directory size is %d," +
                " but the following smaller new SN storage directory sizes" +
                " were specified: %s";
            notes.add(String.format(msg, smallestExistingSize, smallerSizes));
        }
    }

    /**
     * Attempt to assign RNs to SNs, creating and moving RNs where
     * needed. Update both the relationship descriptors and the candidate
     * topology.
     *
     * @return true if successful
     */
    private boolean layoutShards(TopologyCandidate candidate,
                                 StoreDescriptor currentLayout,
                                 int desiredMaxShards) {

        final Topology candidateTopo = candidate.getTopology();

        /* Figure out how many shards we are placing */
        final int numNewShards = desiredMaxShards -
            candidateTopo.getRepGroupMap().size();

        for (int i = 1; i <= numNewShards; i++) {

            /* Create a RepGroup to represent the shard. */
            final RepGroup repGroup = new RepGroup();
            candidateTopo.add(repGroup);

            /* Add RNs for each data center */
            for (final DCDescriptor dcDesc : currentLayout.getDCDesc()) {
                final int repFactor = dcDesc.getRepFactor();
                if (repFactor == 0) {
                    /* Skip Arbiter only Datacenters */
                    continue;
                }

                final List<SNDescriptor> snTargets =
                    layoutOneShard(candidate, dcDesc,
                                   repGroup.getResourceId(), true);

                /*
                 * Before we add this shard to the topology, check that there
                 * are at least <repfactor> number of SNs available for this
                 * shard. If not, we couldn't lay out the shard. That may
                 * happen, since we may not be able to create the ideal maximum
                 * number of shards.
                 */
                if (snTargets.size() != repFactor) {
                    /* Give up, we couldn't place a shard. */
                    return false;
                }

            }
            candidate.log("added shard " + repGroup.getResourceId());
        }

        /*
         * If arbiters are present, add them.
         */
        if (arbitersEnabled && !layoutArbiters(candidate, currentLayout)) {
            return false;
        }

        /*
         * Shards, RNs, and ANs are created, copy storage directories from the
         * descriptors to the RNs. Then distribute partitions.
         */
        distributePartitions(copyStorageDirs(candidate, currentLayout));
        return true;
    }

    /**
     * (Re)Distributes partitions to the shards in the candidate topology.
     * If the candidate's partitions are uninitialized, partitions will be
     * created and assigned to shards in sequential order (see note below).
     *
     * If partitions are already assigned, this will redistribute partitions
     * based on changes to the topology, such as shard addition/removal,
     * change in storage directory sizes, etc.
     */
    private void distributePartitions(TopologyCandidate candidate) {

        final Topology candidateTopo = candidate.getTopology();

        /*
         * totalShards is the number of shards in the topology minus the number
         * of to-be-removed shards
         */
        final int totalShards =
                candidateTopo.getRepGroupMap().size() - removedRgIds.size();
        if (totalShards == 0) {
            return;
        }

        /*
         * Create a set of group descriptors based on the candiadate. The
         * descriptors contain the set of partitions already present in the
         * shard, and a target number of partitions.
         */
        final SortedMap<RepGroupId, ShardDescriptor> shards =
                                         Rules.createShardMap(candidate,
                                                              params,
                                                              removedRgIds,
                                                              numPartitions);

        /*
         * If the partition map is empty, this is a new store, simply assign
         * partitions based on the target #s
         */
        if (candidateTopo.getPartitionMap().isEmpty()) {

            /*
             * Note that when deploying a new store, the partition assignments
             * are not known to the deploy plan (specifically the AddPartitions
             * task). Since the actually partition # assignments are made in
             * that task, it would be good (lest surprise) if this assignment
             * matches what AddPartitions does. Basically, partition #s are
             * assigned sequentially to shards in order of shard #. Since
             * shards is a sorted by group ID, all is well.
             */
            for (ShardDescriptor sd : shards.values()) {
                for (int i = 0; i < sd.getTargetNumPartitions(); i++) {
                    final Partition p = new Partition(sd.getGroupId());
                    candidateTopo.add(p);
                }
            }
            assert candidateTopo.getPartitionMap().size() == numPartitions;
            return;
        }

        /* If only one, nothing to do */
        if (shards.size() == 1) {
            return;
        }

        /* Sorts the shards so most under utilized are first */
        final TreeSet<ShardDescriptor> sortedShards =
                                    new TreeSet<>(shards.values());

        /*
         * The object is to balance the shards so that they are all about even
         * with regard to how over or under the shards are to their optimum
         * size.
         */
        while (true) {
            final ShardDescriptor over = sortedShards.pollLast();
            final ShardDescriptor under = sortedShards.pollFirst();

            /*
             * If moving one partition to the under utilized shard results in
             * it being as bad is the over, then optimization is done.
             */
            if ((under.getOverUnder() + 1) >= over.getOverUnder()) {
                break;
            }

            /* Move one partition and update the topology */
            final PartitionId moveTarget = over.removePartition();
            under.addPartition(moveTarget);
            sortedShards.add(over);
            sortedShards.add(under);
            candidateTopo.update(moveTarget, new Partition(under.getGroupId()));
        }

        /*
         * Check to make sure that no partitions are assigned to shards being
         * removed. This should not happen, but too important not to check.
         */
        for (Partition p : candidateTopo.getPartitionMap().getAll()) {
            if (shards.get(p.getRepGroupId()).isTargetForRemoval()) {
                throw new IllegalStateException("Internal layout error, not " +
                                                "all paritions removed from " +
                                                p.getRepGroupId());
            }
        }
    }

    /**
     * Assign RNs to SNs for a single shard in a single data center and update
     * the topology relationship descriptors. If there are not sufficient
     * resources to accommodate the required RN, the returned list size will be
     * less than the data center's repFactor.
     */
    private List<SNDescriptor> layoutOneShard(TopologyCandidate candidate,
                                              DCDescriptor dcDesc,
                                              RepGroupId rgId,
                                              boolean updateTopology) {

        final int repFactor = dcDesc.getRepFactor();
        final List<SNDescriptor> snsForShard = new ArrayList<>();

        for (int i = 1; i <= repFactor; i++) {

            boolean snFound = false;
            /* The rId is generated and used only if not updating the topo. */
            final RepNodeId rId =
                updateTopology ? null : new RepNodeId(rgId.getGroupId(), i);

            /* Storage directories are sorted, largest being the last entry */
            NavigableMap<StorageDirectory, SNDescriptor> availableStorageDirs =
                    dcDesc.getAvailableStorageDirs();

            /*
             * Currently we have not exposed rnlogdirsize to user hence all
             * values are same i.e. 0L
             */
            NavigableMap<LogDirectory, SNDescriptor> availableRNLogDirs =
                    dcDesc.getAvailableRNLogDirs();

            while (!availableStorageDirs.isEmpty()) {
                final Entry<StorageDirectory, SNDescriptor> e =
                                        availableStorageDirs.pollLastEntry();
                Entry<LogDirectory, SNDescriptor> rnloge =
                                        availableRNLogDirs.pollLastEntry();
                final SNDescriptor snDesc = e.getValue();
                final StorageDirectory sd = e.getKey();

                /*
                 * Case when storage directories are mentioned but not
                 * RN log directories else we will hit NPE on rnloge
                 */
                LogDirectory rnlogd;
                SNDescriptor rnDesc;
                if (rnloge != null) {
                    /*
                     * We have got an SNDescriptor but need to check if snId
                     * are same for snDesc and rnDesc. If not try finding a
                     * snId for rnDesc which matches.
                     */
                    rnDesc = rnloge.getValue();
                    rnlogd = rnloge.getKey();
                    while(rnDesc.getSNId() != snDesc.getSNId() &&
                          (rnloge = availableRNLogDirs.pollLastEntry())
                              != null) {
                        rnDesc = rnloge.getValue();
                        rnlogd = rnloge.getKey();
                    }
                } else {
                    rnlogd = new LogDirectory(null, 0L);
                }

                if (snDesc.isFull()) {
                    continue;
                }
                logger.log(Level.FINEST,
                           "Trying to add RN for shard {0} to {1}",
                           new Object[]{rgId, snDesc});

                if (snDesc.canAdd(rgId, false)) {
                    if (updateTopology) {
                        final RepNode newRN = new RepNode(snDesc.getId());
                        RepGroup repGroup = candidate.getTopology().get(rgId);
                        repGroup.add(newRN);
                        snDesc.add(newRN.getResourceId(), sd);
                        snDesc.add(newRN.getResourceId(), rnlogd);
                    } else {
                        snDesc.add(rId, sd);
                    }
                    snsForShard.add(snDesc);

                    /*
                     * We're done with this RN, since we found an SN. Go on
                     * to the next RN in the shard.
                     */
                    snFound = true;
                    break;
                }
            }

            if (snFound) {
                /* Go on to the next RN in the shard. */
                continue;
            }

            /*
             * Couldn't house this RN, and there are no more free slots anywhere
             * in the store, so give up on this shard.
             */
            availableStorageDirs = dcDesc.getAvailableStorageDirs();
            if (availableStorageDirs.isEmpty()) {
                break;
            }

            /*
             * Couldn't house this RN, but there are still SNs with free
             * slots around.  Try to swap a RN off one of the full SNs onto one
             * of the capacious ones, so we can take that slot.
             */
            for (SNDescriptor fullSND : dcDesc.getSortedSNs()) {
                if (!fullSND.isFull()) {
                    continue;
                }

                /*
                 * TODO : Yet to add enhancement around following case for
                 * rnlogdir.
                 */
                if (swapRNToEmptySlot(fullSND, availableStorageDirs,
                                      rgId, rId, candidate,
                                      updateTopology)) {
                    snsForShard.add(fullSND);
                    snFound = true;
                    break;
                }
            }

            /*
             * We tried to do some swapping, and still couldn't house this SN.
             * End the attempt to place this shard.
             */
            if (!snFound) {
                break;
            }
        }

        if (snsForShard.size() == repFactor) {
            candidate.log("shard " + rgId +
                          " successfully assigned to " +  snsForShard);
        } else {
            candidate.log("shard " + rgId +
                          " incompletely assigned to " +  snsForShard);
        }
        return snsForShard;
    }

    /**
     * Attempt to move one of the RNs on fullSND to a currently empty slot, to
     * make room for an RN from the targetRG.
     *
     * @param fullSND - SN descriptor to move RN from
     * @param available - map of available SNs
     * @param targetRG - repgroup of the RN we want to move
     * @param tmpId - repnode id to place; not used if updateTopology==true
     * @param candidate - topology candidate
     * @param updateTopology - true update topology, false do not update topo
     *                         and use tmpId as resource identifier.
     * @return whether a swap was performed successfully
     */
    private boolean
                swapRNToEmptySlot(SNDescriptor fullSND,
                                  Map<StorageDirectory, SNDescriptor> available,
                                  RepGroupId targetRG,
                                  RepNodeId tmpId,
                                  TopologyCandidate candidate,
                                  boolean updateTopology)  {

        /* Get the RNs that, if moved, the target could take their slot. */
        final Map<RepNodeId, StorageDirectory> swapCandidates =
                            fullSND.getSwapCandidates(targetRG);

        for (Entry<RepNodeId, StorageDirectory> e : swapCandidates.entrySet()) {
            final RepNodeId victimRN = e.getKey();
            final StorageDirectory victimSD = e.getValue();

            /*
             * If the RN was existing in the original topology and the rf is
             * one or two and not using Arbiters, then don't move. This is
             * because the current implementation for moving an RN requires
             * the shutdown of the RN before modifying JEHA. The shutdown
             * of the RN in these cases prevent the JEHA modification due to
             * the lack of sufficient node to acknowledge the transaction.
             */
            if (sourceCandidate.getTopology().get(victimRN) != null &&
                totalPrimaryRF <= 2 &&
                !arbitersEnabled) {
                continue;
            }

            final ShardDescriptor victimShard =
                        fullSND.getShard(new RepGroupId(victimRN.getGroupId()));

            /*
             * If there is no shard descriptor, the victim is a newly placed
             * RN. In that case use the size of the current storage directory
             * as the min.
             */
            final long victimMinDirSize =
                           (victimShard == null) ? victimSD.getSize() :
                                                   victimShard.getMinDirSize();

            /* Find the smallest destination that will host the victim. */
            SNDescriptor bestSN = null;
            StorageDirectory bestSD = null;

            for (Entry<StorageDirectory, SNDescriptor> e2 :
                                                         available.entrySet()) {
                final SNDescriptor destSN = e2.getValue();
                final StorageDirectory destSD = e2.getKey();

                /*
                 * If the victim can be added to the destination and the
                 * directory size is adequate remember it.
                 */
                if (destSN.canAdd(victimRN) &&
                    (destSD.getSize() >= victimMinDirSize)) {

                    /* Remember the smallest suitable destination */
                    if ((bestSD == null) ||
                        (bestSD.getSize() > destSD.getSize())) {
                        bestSN = destSN;
                        bestSD = destSD;
                    }
                }
            }

            if (bestSN != null) {

                /* Change the relationship descriptors */
                bestSN.claim(victimRN, fullSND, bestSD,
                             bestSN.findRNLogDir()/*Check on this*/);

                /* Change the topology */
                final RepNodeId targetRN;
                if (updateTopology) {
                    final RepNode newRN = new RepNode(fullSND.getId());
                    RepGroup repGroup = candidate.getTopology().get(targetRG);
                    repGroup.add(newRN);
                    targetRN = newRN.getResourceId();
                    fullSND.add(targetRN, victimSD);
                    /*TODO : Need to check on this*/
                    fullSND.add(targetRN,fullSND.findRNLogDir());
                    changeSNForRN(candidate.getTopology(), victimRN,
                                  bestSN.getId());
                } else {
                    targetRN = tmpId;
                    fullSND.add(targetRN, victimSD);
                    /*TODO : Need to check on this*/
                    fullSND.add(targetRN,fullSND.findRNLogDir());
                }
                candidate.log("Swap: " + targetRN + " goes to " +
                              fullSND + ", " + victimRN + " goes to " + bestSN);
                return true;
            }
        }
        return false;
    }

    /**
     * Moves the specified number of RNs from the specified SN. Which RNs from
     * the SN are moved is not defined. Returns true if the required number
     * of RNs were moved.
     */
    private boolean moveRNs(StorageNodeId snId,
                            Topology topo,
                            StoreDescriptor currentLayout,
                            int needToMove) {
        final SNDescriptor owningSND = currentLayout.getSND(snId, topo);
        return moveRNs(snId, topo, currentLayout,
                       new ArrayList<>(owningSND.getRNs()), needToMove);
    }

    /**
     * Moves all of the target RNs from the specified SN. Returns true if
     * the all the target RNs were moved.
     */
    private boolean moveRNs(StorageNodeId snId,
                            Topology topo,
                            StoreDescriptor currentLayout,
                            List<RepNodeId> moveTargets) {
        return moveRNs(snId, topo, currentLayout, moveTargets,
                       moveTargets.size());
    }

    /**
     * Move the specified number of RNs in the target list from the specified
     * SN. Which RNs from the target list are moved is not defined. Returns
     * true if the required number of RNs were moved.
     */
    private boolean moveRNs(StorageNodeId snId,
                            Topology topo,
                            StoreDescriptor currentLayout,
                            List<RepNodeId> moveTargets,
                            int needToMove) {
        if (needToMove == 0) {
            return true;
        }
        if (needToMove > moveTargets.size()) {
            return false;
        }
        final SNDescriptor owningSND = currentLayout.getSND(snId, topo);
        final DatacenterId dcId = currentLayout.getOwningDCId(snId, topo);

        /* Check if moves are allowed */
        if (!moveTargets.isEmpty() && !canMoveRN(dcId, topo)) {
            String errorMessage = "Cannot relocate RN when the repfactor is " +
                                  totalPrimaryRF;
            /*
             * If totalPrimaryRF is 2 when RNs cannot be relocated, it means
             * the arbiter is disabled.
             */
            if (totalPrimaryRF == 2) {
                errorMessage += " and arbiter is disabled";
            }

            throw new IllegalCommandException(errorMessage,
                                              ErrorMessage.NOSQL_5200,
                                              CommandResult.
                                              NO_CLEANUP_JOBS);
        }
        final List<SNDescriptor> possibleSNDs = currentLayout.getAllSNDs(dcId);

        for (RepNodeId rnToMove : moveTargets) {
            for (SNDescriptor snd : possibleSNDs) {
                logger.log(Level.FINEST,
                           "Trying to move {0} to {1}",
                           new Object[]{rnToMove, snd});
                /*
                 * findStorageDir() will return null if the SN is not in the
                 * pool
                 */
                final StorageDirectory sd = snd.findStorageDir(rnToMove);
                final LogDirectory rld = snd.findRNLogDir();
                if (sd != null) {
                    /* Move the RN in the descriptions. */
                    snd.claim(rnToMove, owningSND, sd, rld);
                    changeSNForRN(topo, rnToMove, snd.getId());
                    if (--needToMove == 0) {
                        return true;
                    }
                    break;
                }
            }
        }
        return false;
    }

    /**
     * Returns true if we can move RNs in the specified zone.
     */
    private boolean canMoveRN(DatacenterId dcId, Topology topo) {
        return (totalPrimaryRF > 2) || arbitersEnabled ||
               topo.get(dcId).getDatacenterType().isSecondary();
    }

    /** Change the SN for this RN in topology */
    private void changeSNForRN(Topology topo,
                               RepNodeId rnToMove,
                               StorageNodeId snId) {
        logger.log(Level.FINEST, "Swapped {0} to {1}",
                   new Object[]{rnToMove, snId});
        final RepNode updatedRN = new RepNode(snId);
        final RepGroupId rgId = topo.get(rnToMove).getRepGroupId();
        final RepGroup rg = topo.get(rgId);
        rg.update(rnToMove, updatedRN);
    }

    private void addOneRN(Topology topo, SNDescriptor snd,
                          RepGroupId rgId, StorageDirectory sd,
                          LogDirectory rld) {
        /* Add an RN in the candidate topology */
        final RepNode newRN = new RepNode(snd.getId());
        final RepNode added = topo.get(rgId).add(newRN);

        /* Add this new RN to the SN's list of hosted RNs */
        snd.add(added.getResourceId(), sd);
        snd.add(added.getResourceId(), rld);
    }

    /**
     * Attempt to add RNs to this shard, in this datacenter, to bring its
     * rep factor up to snuff.
     */
    private void addRNs(Topology topo,
                        DatacenterId dcId,
                        RepGroupId rgId,
                        StoreDescriptor currentLayout,
                        int numNeeded) {

        final List<SNDescriptor> possibleSNDs = currentLayout.getAllSNDs(dcId);

        int fixed = 0;
        for (int i = 0; i < numNeeded; i++) {
            /* Try one time for each RN we need to add to this shard. */
            boolean success = false;
            for (SNDescriptor snd : possibleSNDs) {
                logger.log(Level.FINEST,
                           "Trying add an RN to {0} on {1}",
                           new Object[]{rgId, snd});
                final StorageDirectory sd = snd.findStorageDir(rgId);
                final LogDirectory rld = snd.findRNLogDir();
                if (sd != null) {
                    addOneRN(topo, snd, rgId, sd, rld);
                    fixed++;
                    success = true;
                    break;
                }
            }
            if (!success) {
                /*
                 * We went through all the available SNs once, and none of them
                 * could house something from this shard, so give up.
                 */
                break;
            }
        }

        /*
         * If some couldn't be added, try again, this time consenting to move
         * existing RNs.
         */
        final int remaining = numNeeded - fixed;
        for (int i = 0; i < remaining; i++) {
            boolean success = false;
            for (SNDescriptor snd : possibleSNDs) {
                if (!snd.hosts(rgId)) {
                    /* Will move any RN away */
                    moveRNs(snd.getId(), topo, currentLayout, 1);
                    final StorageDirectory sd = snd.findStorageDir(rgId);
                    final LogDirectory rld = snd.findRNLogDir();
                    if (sd != null) {
                        addOneRN(topo, snd, rgId, sd, rld);
                        fixed++;
                        success = true;
                        break;
                    }
                }
            }
            if (!success) {
                /* Give up, can't house something from this shard anywbere */
                break;
            }
        }
    }

    /**
     * Remove RNs from a shard in a secondary zone to match the requested
     * replication factor. Removes the numExcess RNs with the highest RN IDs.
     */
    private void removeRNs(final Topology topo,
                           final DatacenterId dcId,
                           final RepGroupId rgId,
                           final StoreDescriptor currentLayout,
                           final int numExcess) {

        if (!topo.get(dcId).getDatacenterType().isSecondary()) {
            throw new IllegalArgumentException(
                "Can't remove RNs from zone " + dcId + ", which is not a" +
                " secondary zone");
        }

        final List<SNDescriptor> snDescriptors =
            currentLayout.getAllSNDs(dcId);
        for (int i = 0; i < numExcess; i++) {
            RepNodeId removeRN = null;
            SNDescriptor removeFromSN = null;
            for (final SNDescriptor snDesc : snDescriptors) {
                for (final RepNodeId rnId : snDesc.getRNs()) {

                    /* Check shard */
                    if (!topo.get(rnId).getRepGroupId().equals(rgId)) {
                        continue;
                    }

                    /* Save highest numbered RN */
                    if ((removeRN == null) || rnId.compareTo(removeRN) > 0) {
                        removeRN = rnId;
                        removeFromSN = snDesc;
                    }
                }
            }
            if ((removeRN == null) || (removeFromSN == null)) {
                throw new IllegalStateException("RN to remove not found");
            }
            topo.remove(removeRN);
            removeFromSN.remove(removeRN);
        }
    }

    /**
     * Add an Arbiter to SND and topo.
     * @param topo
     * @param snd
     * @param rgId
     */
    private void addOneARB(Topology topo, SNDescriptor snd, RepGroupId rgId) {
        /* Add an ARB in the candidate topology */
        final ArbNode newARB = new ArbNode(snd.getId());
        final ArbNode added = topo.get(rgId).add(newARB);

        /* Add this new Arb to the SN's list of hosted ARBs */
        snd.add(added.getResourceId());
    }

    /**
     * Remove arbiter from topo and snd
     */
    private void removeAN(Topology topo,
                          StorageNodeId snId,
                          StoreDescriptor currentLayout,
                          ArbNodeId anId) {

        final SNDescriptor snd = currentLayout.getSND(snId, topo);
        snd.remove(anId);
        topo.remove(anId);
    }

    /**
     * Move AN to different SN in the specified DC.
     * May or may not perform the move.
     *
     * @param topo topology object
     * @param snId SN hosting AN to move
     * @param currentLayout current layout descripter
     * @param anId AN to move
     * @param dcId DC to move AN to
     * @return whether the move succeeded
     */
    private boolean moveAN(Topology topo,
                           StorageNodeId snId,
                           StoreDescriptor currentLayout,
                           ArbNodeId anId,
                           DatacenterId dcId) {
        RepGroup rg = topo.get(topo.get(anId).getRepGroupId());
        SNDescriptor owningSND = currentLayout.getSND(snId, topo);
        SNDescriptor newSN =
            findSNForArbiter(currentLayout, rg, dcId);
        if (newSN != null) {
            newSN.claim(anId, owningSND);
            changeSNForAN(topo, anId, newSN.getId());
            return true;
        }
        return false;
    }

    /** Change the SN for this AN in topology */
    private void changeSNForAN(Topology topo,
                               ArbNodeId anToMove,
                               StorageNodeId snId) {
        logger.log(Level.FINEST, "Swapped {0} to {1}",
                   new Object[]{anToMove, snId});
        final ArbNode updatedAN = new ArbNode(snId);
        final RepGroupId rgId = topo.get(anToMove).getRepGroupId();
        final RepGroup rg = topo.get(rgId);
        rg.update(anToMove, updatedAN);
    }

    /**
     * Add AN to specified DC. May or may not be able to.
     *
     * @param topo
     * @param dcId
     * @param rgId
     * @param currentLayout
     * @return whether the addition was successful
     */
    private boolean addAN(Topology topo,
                          DatacenterId dcId,
                          RepGroupId rgId,
                          StoreDescriptor currentLayout) {
        RepGroup rg = topo.get(rgId);
        SNDescriptor sn = findSNForArbiter(currentLayout, rg, dcId);
        if (sn != null) {
            addOneARB(topo, sn, rgId);
            return true;
        }
        return false;
    }

    /**
     * Assigns storage directories to RNs in the specified candidate.
     */
    private TopologyCandidate
                         assignStorageDirectories(TopologyCandidate candidate) {
        return copyStorageDirs(candidate,
                               new StoreDescriptor(candidate, params,
                                                   snPool.getList()));
    }

    /**
     * Copies the storage and log directories assignments from the descriptors
     * to the candidate topology. Sanity check that a storage and rn log
     * directory was assigned and that only one RN has been assigned per
     * storage and rn log directory. Shouldn't happen, but contain any errors
     * here, rather than letting a bad candidate be propagated.
     */
    private TopologyCandidate copyStorageDirs(TopologyCandidate candidate,
                                              StoreDescriptor currentLayout) {
        final Map<String, RepNodeId> storageDirToRN = new HashMap<>();

        final Topology candTopo = candidate.getTopology();
        for (RepNode rn : candTopo.getSortedRepNodes()) {
            final RepNodeId rnId = rn.getResourceId();
            final StorageNodeId snId = rn.getStorageNodeId();
            final SNDescriptor snDesc = currentLayout.getSND(snId, candTopo);
            final StorageDirectory assignedSD = snDesc.getStorageDir(rnId);
            if (assignedSD == null) {
                throw new CommandFaultException
                    ("Internal error, topology candidate " +
                     candidate.getName() + " is invalid because " +
                     rnId + " on " + snId + " is missing its storage directory",
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }

            candidate.saveDirectoryAssignment(rnId, assignedSD.getPath());

            final LogDirectory assignedLD = snDesc.getRNLogDir(rnId);
            if (assignedLD == null) {
                throw new CommandFaultException
                    ("Internal error, topology candidate " +
                     candidate.getName() + " is invalid because " +
                     rnId + " on " + snId + " is missing its rn log directory",
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }

            candidate.saveRNLogDirectoryAssignment(rnId, assignedLD.getPath());

            if (assignedSD.isRoot()) {
                continue;
            }

            final String key = snId + assignedSD.getPath();
            final RepNodeId clashingRN =  storageDirToRN.get(key);
            if (clashingRN != null) {
                throw new CommandFaultException
                    ("Topology candidate " + candidate.getName() +
                     " is invalid because " + clashingRN + " and " +
                     rnId + " are both assigned to " + snId +
                     ", " + assignedSD,
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
            storageDirToRN.put(key, rnId);
        }

        /* a sanity check, as much to guard against bugs. */
        Rules.checkAllStorageDirsExist(candidate, params);
        Rules.checkAllRNLogDirsExist(candidate, params);

        return candidate;
    }

    /**
     * Return the SNs sorted by SNId.
     */
    private List<SNDescriptor> getSortedSNs(List<SNDescriptor> in) {
        final List<SNDescriptor> snList = new ArrayList<>(in);
        Collections.sort(snList, new Comparator<SNDescriptor>() {
            @Override
                public int compare(SNDescriptor snA,
                                   SNDescriptor snB) {
                    return snA.getId().getStorageNodeId() -
                        snB.getId().getStorageNodeId();
            }});
        return snList;
    }

    private List<SNDescriptor> getOverMaxSNs(Topology topo,
                                             StoreDescriptor layout,
                                             DatacenterId dcId) {
        int maxNumAN =
            ArbiterTopoUtils.computeZoneMaxANsPerSN(topo, params, dcId);
        List<SNDescriptor> retVal = new ArrayList<>();
        for (SNDescriptor snd : layout.getAllSNDs(dcId)) {
            if (!snd.getAllowArbiters()) {
                continue;
            }
            if (snd.getARBs().size() > maxNumAN) {
                retVal.add(snd);
            }
        }
        return retVal;
    }

    private List<SNDescriptor> getUnderAvgSNs(Topology topo,
                                              StoreDescriptor layout,
                                              DatacenterId dcId) {
        int maxNumAN =
            ArbiterTopoUtils.computeZoneMaxANsPerSN(topo, params, dcId);
        List<SNDescriptor> retVal = new ArrayList<>();
        for (SNDescriptor snd : layout.getAllSNDs(dcId)) {
            if (!snd.getAllowArbiters()) {
                continue;
            }
            if (snd.getARBs().size() < maxNumAN) {
                retVal.add(snd);
            }
        }
        return retVal;
    }

    /*
     * Make one pass over the SN's hosting arbiters in order to
     * allocate ANs with an even distribution. Simple pass looking for
     * SNs with more than the computed average number of ANs. Attempts to
     * move AN to other potential SN's that have AN below the average number.
     */
    private void balanceAN(Topology topo,
                           StoreDescriptor layout,
                           DatacenterId dcId) {
        int maxNumAN =
            ArbiterTopoUtils.computeZoneMaxANsPerSN(topo, params, dcId);
        if (maxNumAN == 0) {
            /* There are no usable SNs */
            return;
        }
        List<SNDescriptor> overSNs = getOverMaxSNs(topo, layout, dcId);
        for (SNDescriptor snd : overSNs) {
            int numberToMove =  snd.assignedARBs.size() - maxNumAN;
            int movedAN = 0;
            Set<ArbNodeId> assignedANs = new TreeSet<>(snd.assignedARBs);
            for (ArbNodeId anId : assignedANs) {
                if (movedAN == numberToMove) {
                    break;
                }
                List<SNDescriptor> under =
                    getUnderAvgSNs(topo, layout, dcId);
                ArbNode an = topo.get(anId);
                for (SNDescriptor checkSN : under) {
                    if (!checkSN.hosts(an.getRepGroupId())) {
                        logger.log(Level.FINE,
                                   "balanceAN moving AN. System mean {0} " +
                                   "current SN {1} num ANs {2} " +
                                   "to SN {3} numANs {4}.",
                                    new Object[]{maxNumAN, snd.getId(),
                                                 snd.assignedARBs.size(),
                                                 checkSN.getId(),
                                                 checkSN.assignedARBs.size()});
                        checkSN.claim(anId, snd);
                        changeSNForAN(topo, anId, checkSN.getId());
                        movedAN++;
                        break;
                    }
                }
            }
        }
    }

    /**
     * The top level descriptor of the store and the relationships of the
     * existing topology's components.
     */
    private class StoreDescriptor {

        protected final Map<DatacenterId, DCDescriptor> dcMap = new TreeMap<>();
        private final SortedMap<RepGroupId, ShardDescriptor> shardMap;

        StoreDescriptor(TopologyCandidate candidate,
                        Parameters params,
                        List<StorageNodeId> availableSNIds) {
            final Topology topo = candidate.getTopology();
            for (StorageNodeId snId : availableSNIds) {
                final StorageNode sn = topo.get(snId);
                final DatacenterId dcId = sn.getDatacenterId();

                DCDescriptor dcDesc = dcMap.get(dcId);
                if (dcDesc == null) {
                    final int rf = topo.getDatacenter(snId).getRepFactor();
                    dcDesc = new DCDescriptor(this, dcId, rf);
                    dcMap.put(dcId, dcDesc);
                }
                dcDesc.add(snId, params.get(snId));
            }

            initSNDescriptors(candidate);

            shardMap = Rules.createShardMap(candidate, params);
        }

        /**
         * Initializes the SN descriptors and RN assignments.
         */
        protected void initSNDescriptors(TopologyCandidate candidate) {
            for (DCDescriptor dcDesc: dcMap.values()) {
                dcDesc.initSNDescriptors(candidate);
            }
        }

        Collection<DCDescriptor> getDCDesc() {
            return dcMap.values();
        }

        List<SNDescriptor> getAllSNDs(DatacenterId dcId) {
            final DCDescriptor dc = dcMap.get(dcId);
            return (dc == null) ? Collections.<SNDescriptor> emptyList() :
                                  dc.getSortedSNs();
        }

        DatacenterId getOwningDCId(StorageNodeId snId, Topology topo) {
           return topo.get(snId).getDatacenterId();
        }

        SNDescriptor getSND(StorageNodeId snId, Topology topo) {
            final DatacenterId dcId = getOwningDCId(snId, topo);
            return dcMap.get(dcId).get(snId);
        }

        private ShardDescriptor getShard(RepGroupId rgId) {
            return shardMap.get(rgId);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("StoreDescriptor[\n");
            toString(sb).append("]");
            return sb.toString();
        }

        protected StringBuilder toString(StringBuilder sb) {
            for (DCDescriptor dcd : dcMap.values()) {
                dcd.toString(sb, "\t").append("\n");
            }
            for (ShardDescriptor sd : shardMap.values()) {
                sd.toString(sb, "\t").append("\n");
            }
            return sb;
        }

    }

    /**
     * A set of descriptors that are initialized with the existing physical
     * resources, but not any of the existing topology components, so it looks
     * like a blank, clean store. It's used to calculate the ideal number of
     * shards possible for such a layout.
     */
    private class EmptyLayout extends StoreDescriptor {

        private final int maxShards;

        EmptyLayout(TopologyCandidate candidate,
                    Parameters params,
                    StorageNodePool snPool) {
            super(candidate, params, snPool.getList());
            maxShards = calculateMaxShards
                     (new TopologyCandidate("scratch",
                                            candidate.getTopology().getCopy()));
        }

        /**
         * This method is called by StoreDescriptor constructor to initialize
         * the SN descriptors. This override does not record existing RN
         * assignments so that an optimum calculation can be done.
         */
        @Override
        protected void initSNDescriptors(TopologyCandidate candidate) {
            for (DCDescriptor dcDesc: dcMap.values()) {
                dcDesc.initSNDescriptors(null);
            }
        }

        private String showDatacenters() {
            final StringBuilder sb = new StringBuilder();
            for (final DCDescriptor dcDesc : dcMap.values()) {
                sb.append(DatacenterId.DATACENTER_PREFIX + " id=").append(
                    dcDesc.getDatacenterId());
                sb.append(" maximum shards= ").append(dcDesc.getNumShards());
                sb.append('\n');
            }
            return sb.toString();
        }

        /**
         * Calculate the maximum number of shards this store could support.
         */
        private int calculateMaxShards(TopologyCandidate candidate) {
            int calculatedMax = Integer.MAX_VALUE;
            for (Map.Entry<DatacenterId, DCDescriptor> entry :
                 dcMap.entrySet()) {

                final DCDescriptor dcDesc = entry.getValue();
                if (dcDesc.getRepFactor() == 0) {
                    continue;
                }
                dcDesc.calculateMaxShards(candidate);
                final int dcMax = dcDesc.getNumShards();
                if (calculatedMax > dcMax) {
                    calculatedMax = dcMax;
                }
            }
            return calculatedMax;
        }

        private int getMaxShards() {
            return maxShards;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("EmptyLayout[maxShards=").append(maxShards).append("\n");
            toString(sb).append("]");
            return sb.toString();
        }
    }

    /**
     * Information about the datacenter characteristics and the SNs in that DC.
     */
    private class DCDescriptor {
        private final StoreDescriptor storeDesc;

        /** The data center ID */
        private final DatacenterId dcId;

        /* Configured by the user. */
        private final int repFactor;

        /* The number of shards currently mapped onto this datacenter. */
        private int numShards;

        private final Map<StorageNodeId, SNDescriptor> sns = new TreeMap<>();

        DCDescriptor(final StoreDescriptor storeDesc,
                     final DatacenterId dcId,
                     final int repFactor) {
            this.storeDesc = storeDesc;
            this.dcId = dcId;
            this.repFactor = repFactor;
        }

        /**
         * Gets a map of the available storage directories in this DC. The map
         * is ordered by size of directory. With the largest being the
         * last entry.
         */
        private NavigableMap<StorageDirectory, SNDescriptor>
                                            getAvailableStorageDirs() {
            final NavigableMap<StorageDirectory, SNDescriptor> map =
                                                                new TreeMap<>();

            for (SNDescriptor desc : sns.values()) {

                /* If full, or not in the pool, there are no available slots */
                if (desc.isFull() || !desc.isInPool()) {
                    continue;
                }
                final StorageNodeParams snp = desc.snp;
                final ParameterMap pm = snp.getStorageDirMap();
                /*
                 * If the # of explicit storage directories is less than
                 * capacity add the root dir as an available destination.
                 */
                if ((pm == null) || (pm.size() < snp.getCapacity())) {
                    map.put(new StorageDirectory(null,
                                                 snp.getRootDirSize()),
                                                 desc);
                }

                /* Add non-assigned explicit storage directories */
                if (pm != null) {
                    for (Parameter p : pm) {
                        final StorageDirectory sd = new StorageDirectory(p);
                        if (!desc.isAssigned(sd)) {
                            map.put(sd, desc);
                        }
                    }
                }
            }
            return map;
        }

        /**
         * Gets a map of the available RN log directories in this DC. The map
         * is ordered by size of log directory. With the largest being the
         * last entry.
         */
        private NavigableMap<LogDirectory, SNDescriptor>
                                            getAvailableRNLogDirs() {
            final NavigableMap<LogDirectory, SNDescriptor> map =
                                                              new TreeMap<>();

            for (SNDescriptor desc : sns.values()) {

                /* If full, or not in the pool, there are no available slots */
                if (desc.isRNLogFull() || !desc.isInPool()) {
                    continue;
                }
                final StorageNodeParams snp = desc.snp;
                final ParameterMap pm = snp.getRNLogDirMap();
                /*
                 * If the # of explicit RN log directories is less than
                 * capacity add the root dir as an available destination.
                 */
                if ((pm == null) || (pm.size() < snp.getCapacity())) {
                    map.put(new LogDirectory(null, 0L), desc);
                }

                /* Add non-assigned explicit RN log directories */
                if (pm != null) {
                    for (Parameter p : pm) {
                        final LogDirectory rld = new LogDirectory(p);
                        if (!desc.isAssigned(rld)) {
                            map.put(rld, desc);
                        }
                    }
                }
            }
            return map;
        }

        /**
         * Return the SNs sorted by SNId.
         */
        List<SNDescriptor> getSortedSNs() {
            return new ArrayList<>(sns.values());
        }

        DatacenterId getDatacenterId() {
            return dcId;
        }

        int getRepFactor() {
            return repFactor;
        }

        int getNumShards() {
            return numShards;
        }

        /**
         * Add this SN's params. Needed to get access to information like
         * capacity, and other physical constraints.
         */
        void add(StorageNodeId snId, StorageNodeParams snp) {
            sns.put(snId, new SNDescriptor(storeDesc, snId, snp));
        }

        /**
         * The SNDescriptors describe how shards and RNs map to the SNs.
         * Initialize in preparation for topology building.  If the candidate
         * argument is null, ignore any RNs that are already assigned to an SN.
         * We want to calculate the theoretical ideal layout so we start with a
         * blank slate.
         */
        private void initSNDescriptors(TopologyCandidate candidate) {

            numShards = 0;

            /* Ignore the existing RNs, we want to have a clean slate. */
            if (candidate == null) {
                return;
            }

            for (SNDescriptor snd : getSortedSNs()) {
                snd.clearAssignedRNs();
                snd.clearAssignedRNLogs();
            }

            final Topology topo = candidate.getTopology();
            for (RepNode rn: topo.getSortedRepNodes()) {
                final StorageNodeId snId = rn.getStorageNodeId();
                /* Only add RNs for SNs in this data center */
                if (dcId.equals(topo.get(snId).getDatacenterId())) {
                    final SNDescriptor snd = sns.get(snId);
                    final RepNodeId rnId = rn.getResourceId();
                    final RepNodeParams rnp = params.get(rnId);

                    /*
                     * If the topology has been deployed the RN's directory
                     * assignment should be in the parameters. If it is not
                     * in the parameters then this topology has not yet been
                     * deployed, so use the assignment saved with the candidate.
                     */
                    StorageDirectory sd = (rnp == null) ?
                            candidate.getStorageDir(rnId, params) :
                            new StorageDirectory(rnp.getStorageDirectoryPath(),
                                                 rnp.getStorageDirectorySize());

                    LogDirectory rld = (rnp == null) ?
                            candidate.getRNLogDir(rnId, params) :
                            new LogDirectory(rnp.getLogDirectoryPath(), 0L);

                    /* This should only be null during unit test. Confirm? */
                    if (sd == null) {
                        sd = StorageDirectory.DEFAULT_STORAGE_DIR;
                    }

                    if (rld == null) {
                        rld = LogDirectory.DEFAULT_LOG_DIR;
                    }
                    snd.addInit(rnId, sd);
                    snd.addInit(rnId, rld);
                }
            }

            for (final ArbNode an : topo.getSortedArbNodes()) {
                final StorageNodeId snId = an.getStorageNodeId();
                /* Only add ANs for SNs in this data center */
                if (dcId.equals(topo.get(snId).getDatacenterId())) {
                    SNDescriptor snd = sns.get(an.getStorageNodeId());
                    snd.addInit(an.getResourceId());
                }
            }

            // TODO: not sufficient for multi-datacenters. numShards is not
            // necessarily to be initialed to the the same as the number
            // of shards in the topology, if this datacenter has fewer shards,
            // due to a previous abnormal plan end
            numShards = topo.getRepGroupMap().size();
        }

        /**
         * Each datacenter must have a complete copy of the data in the store
         * and has an individual rep factor requirement, so each datacenter has
         * its own notion of the maximum number of shards it can support.
         * Find out how many more shards can go on this datacenter.
         */
        void calculateMaxShards(TopologyCandidate candidate) {

            if (repFactor == 0) {
                numShards = 0;
                return;
            }

            candidate.log("calculating max number of shards");

            int wholeShards = 0;
            int shardNumber = 0;
            while (true) {

                final List<SNDescriptor> snsForShard =
                    layoutOneShard(candidate,
                                   this, new RepGroupId(++shardNumber), false);
                if (snsForShard.size() < repFactor) {
                    /* Couldn't lay out a complete shard this time, stop. */
                    break;
                }

                wholeShards++;
            }
            numShards += wholeShards;
        }

        SNDescriptor get(StorageNodeId snId) {
            return sns.get(snId);
        }

        @Override
        public String toString() {
            return toString(new StringBuilder(), "").toString();
        }

        private StringBuilder toString(StringBuilder sb, String tab) {
            sb.append(tab).append("DCDescriptor[").append(dcId);
            sb.append(" rf=").append(repFactor);
            sb.append(" #shards=").append(numShards).append("\n");
            for (SNDescriptor snd : sns.values()) {
                sb.append(tab).append("\t").append(snd.toString()).append("\n");
            }
            sb.append(tab).append("]");
            return sb;
        }
    }

    /**
     * Keeps track of the RNs assigned to an SN, consulting Rules for
     * permitted placements.
     */
    private class SNDescriptor {
        private final StoreDescriptor storeDesc;
        private final StorageNodeId snId;
        private final StorageNodeParams snp;
        private final Map<RepNodeId, StorageDirectory> assignedRNs =
                                                                new TreeMap<>();
        private final Map<RepNodeId, LogDirectory> assignedLogRNs =
                                                                new TreeMap<>();
        private final Set<ArbNodeId> assignedARBs = new TreeSet<>();
        private final boolean inPool;

        SNDescriptor(StoreDescriptor storeDesc,
                     StorageNodeId snId, StorageNodeParams snp) {
            this.storeDesc = storeDesc;
            this.snId = snId;
            this.snp = snp;
            inPool = snPool.contains(snId);
        }

        private boolean hosts(RepGroupId rgId) {
            for (RepNodeId rnId : assignedRNs.keySet()) {
                if (rnId.getGroupId() == rgId.getGroupId()) {
                    return true;
                }
            }
            return false;
        }

        private boolean isInPool() {
            return inPool;
        }

        private StorageNodeId getSNId() {
            return snId;
        }

        /**
         * Returns true if the specified RN can be assigned to this SN.
         */
        private boolean canAdd(RepNodeId rId) {
            return canAdd(new RepGroupId(rId.getGroupId()), false);
        }

        /**
         * Returns true if an RN in the specified group can be assigned to
         * this SN. If ignoreCapacity is true, the SN capacity limit is
         * ignored.
         */
        private boolean canAdd(RepGroupId rgId, boolean ignoreCapacity) {
            return inPool ? Rules.checkRNPlacement(snp, assignedRNs.keySet(),
                                                   rgId.getGroupId(),
                                                   ignoreCapacity,
                                                   logger) :
                            false;
        }

        /**
         * Returns a suitable storage directory for the specified RN.
         * If an RN in the group can not be assigned to this SN or no
         * storage directory is found null is returned.
         */
        private StorageDirectory findStorageDir(RepNodeId rId) {
            return findStorageDir(new RepGroupId(rId.getGroupId()));
        }

        /**
         * Returns a suitable storage directory for a node in the specified
         * group. If an RN in the group can not be assigned to this SN or no
         * storage directory is found null is returned.
         */
        private StorageDirectory findStorageDir(RepGroupId rgId) {
            if (!canAdd(rgId, false)) {
                return null;
            }

            final ShardDescriptor sd = storeDesc.getShard(rgId);
            final long size = (sd == null) ? 0L : sd.getMinDirSize();
            return Utils.findStorageDir(snp, size, assignedRNs);
        }

        /**
         * Returns a suitable RN log directory for a node. findRNLogDir
         * assumes that RN can be assigned to SN. If explicit RN log
         * directory is not found then we put in root directory
         */
        private LogDirectory findRNLogDir() {

            /*
             * If there are defined RN log directories, try to find one
             * that works
             */
            final ParameterMap rnlogDirMap = snp.getRNLogDirMap();
            if (rnlogDirMap != null) {
                for (Parameter p : rnlogDirMap) {
                    final LogDirectory proposed = new LogDirectory(p);
                    if (!assignedLogRNs.containsValue(proposed)) {
                        return proposed;
                    }
                }
            }

            /* No explicit RN log directories found, put in root. */
            return LogDirectory.DEFAULT_LOG_DIR;
        }

        /**
         * Returns the storage directories on this SN which are currently
         * assigned, but could host an RN in the specified group.
         */
        private Map<RepNodeId, StorageDirectory>
                                            getSwapCandidates(RepGroupId rgId) {
            if (!canAdd(rgId, true)) {
                return Collections.<RepNodeId, StorageDirectory>emptyMap();
            }
            final Map<RepNodeId, StorageDirectory> ret = new HashMap<>();
            final ShardDescriptor sd = storeDesc.getShard(rgId);
            final long size = (sd == null) ? 0L : sd.getMinDirSize();
            for (Entry<RepNodeId, StorageDirectory> e : assignedRNs.entrySet()){
                if (e.getValue().getSize() >= size) {
                    ret.put(e.getKey(), e.getValue());
                }
            }
            return ret;
        }

        private StorageDirectory getStorageDir(RepNodeId rnId) {
            return assignedRNs.get(rnId);
        }

        private LogDirectory getRNLogDir(RepNodeId rnId) {
            return assignedLogRNs.get(rnId);
        }

        private boolean isAssigned(StorageDirectory sd) {
            return assignedRNs.containsValue(sd);
        }

        private boolean isAssigned(LogDirectory sd) {
            return assignedLogRNs.containsValue(sd);
        }

        /**
         * Check for >= rather than == capacity in case the SN's capacity value
         * is changed during the topo building. In theory, that should be
         * prohibited, but be conservative on the check.
         */
        private boolean isFull() {
            return assignedRNs.size() >= snp.getCapacity();
        }

        private boolean isRNLogFull() {
            return assignedLogRNs.size() >= snp.getCapacity();
        }

        /**
         * Adds the specified RN to this SN, assigned to the storage directory.
         */
        private void add(RepNodeId rnId, StorageDirectory sd) {
            assert inPool;
            addInit(rnId, sd);
        }

        /**
         * Adds the specified RN log to this SN, assigned to the RN
         * log directory.
         */
        private void add(RepNodeId rnId, LogDirectory rld) {
            assert inPool;
            addInit(rnId, rld);
        }

        /**
         * Removes the specified RN, throwing an exception if the RN is not
         * present.
         */
        private void remove(RepNodeId rnId) {
            if (assignedRNs.remove(rnId) == null) {
                throw new IllegalArgumentException(
                    "RN " + rnId + " was not present");
            }
        }

        /* Internal add to bypass in pool check */
        private void addInit(RepNodeId rnId, StorageDirectory sd) {
            assignedRNs.put(rnId, sd);
        }

        /* Internal add to bypass in pool check */
        private void addInit(RepNodeId rnId, LogDirectory rld) {
            assignedLogRNs.put(rnId, rld);
        }

        private void add(ArbNodeId aId) {
            assert inPool;
            assignedARBs.add(aId);
        }

        /* Internal add to bypass in pool check */
        private void addInit(ArbNodeId aId) {
            assignedARBs.add(aId);
        }

        private void remove(ArbNodeId aId) {
            assignedARBs.remove(aId);
        }

        StorageNodeId getId() {
            return snId;
        }

        void clearAssignedRNs() {
            assignedRNs.clear();
        }

        void clearAssignedRNLogs() {
            assignedLogRNs.clear();
        }

        /** Move RN from owning SNDescriptor to this one. */
        void claim(RepNodeId rnId, SNDescriptor owner, StorageDirectory sd,
                   LogDirectory rld) {
            assert inPool;
            owner.assignedRNs.remove(rnId);
            owner.assignedLogRNs.remove(rnId);
            add(rnId, sd);
            add(rnId, rld);
            logger.log(Level.FINE,
                       "Moved {0} from {1} to {2}",
                       new Object[]{rnId, owner, this});
        }

        /** Move AN from owning SNDescriptor to this one. */
        void claim(ArbNodeId anId, SNDescriptor owner) {
            assert inPool;
            owner.assignedARBs.remove(anId);
            assignedARBs.add(anId);
            logger.log(Level.FINE,
                       "Moved {0} from {1} to {2}",
                       new Object[]{anId, owner, this});
        }

        Set<RepNodeId> getRNs() {
            return assignedRNs.keySet();
        }

        Set<ArbNodeId> getARBs() {
            return assignedARBs;
        }

        int getCapacity() {
            return snp.getCapacity();
        }

        boolean getAllowArbiters() {
            return snp.getAllowArbiters();
        }

        private ShardDescriptor getShard(RepGroupId repGroupId) {
            return storeDesc.getShard(repGroupId);
        }

        @Override
        public String toString() {
            return "SNDescriptor[" + snId + " hosted RNs=" + assignedRNs +
                   assignedLogRNs + " capacity=" + snp.getCapacity() +
                   " hosted ANs=" + assignedARBs + " inPool=" + inPool + "]";
        }
    }
}
