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

package oracle.kv.impl.admin.plan.task;

import static oracle.kv.impl.topo.Datacenter.MASTER_AFFINITY;
import static oracle.kv.impl.topo.Datacenter.NO_MASTER_AFFINITY;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.admin.topo.LogDirectory;
import oracle.kv.impl.admin.topo.StorageDirectory;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.rep.NodeType;

/**
 * Create and start a single RepNode on a particular Storage Node. This
 * requires:
 *
 * 1. adding the new RN to the topology
 * 2. adding appropriate param entries to the AdminDB for this RN.
 * 3. contacting the owning SN to invoke the RN creation.
 *
 * Note that since the Admin DB has the authoritative copy of the topology and
 * metadata, (1) and (2) must be done before the remote request to the SN is
 * made. The task must take care to be idempotent. Topology changes should not
 * be made needlessly, because unnecessary versions merely need pruning later.
 */
public class DeployNewRN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final DeployTopoPlan plan;
    private final StorageNodeId snId;
    private final String snDescriptor;

    /*
     * Note that we now use "storage directory" instead of "mount point", but
     * since the field is serialized it would be a pain to change.
     */
    private final String mountPoint;

    /*
     * If deserializing from an old version storageDirectorySize will
     * be 0. It is assumed that checks have already been made to prevent
     * a non-zero size before the Admins are upgraded.
     */
    private final long storageDirectorySize;

    /*
     * RN log directory
     */
    private final String logDirectory;

    /*
     * RN log directory size
     */
    private final long logDirectorySize;

    /*
     * Since the RepNodeId is only calculated when the task executes, this
     * field may be null. However, go to the effort to hang onto this
     * when it's available, because it's very useful for logging.
     */
    private RepNodeId displayRNId;

    /*
     * Only one of these fields will be set. If the RN is being made for a
     * brand new shard, the RepGroupId hasn't been allocated at the time when
     * the task is constructed. In that case, we use the planShardIdx. If
     * the RN is being added to a shard that already exists, the specified
     * shard field will be set.
     */
    private int planShardIdx;
    private RepGroupId specifiedShard;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<String> FAULT_HOOK;

    /**
     * Creates a task for creating and starting a new RepNode for a brand
     * new shard, when we don't yet know the shard's id
     * @param storageDirectory if null, put RN in the SN's root directory
     * @param logDirectory if null, put RN logs in the SN's root directory
     */
    public DeployNewRN(DeployTopoPlan plan,
                       StorageNodeId snId,
                       int planShardIdx,
                       StorageDirectory storageDirectory,
                       LogDirectory logDirectory) {
        this(plan, snId, storageDirectory, logDirectory);
        this.planShardIdx = planShardIdx;
    }

    /**
     * Creates a task for creating and starting a new RepNode for a shard
     * that already exists and has a repGroupId.
     * @param storageDirectory if null, put RN in the SN's root directory
     * @param logDirectory if null, put RN logs in the SN's root directory
     */
    public DeployNewRN(DeployTopoPlan plan,
                       StorageNodeId snId,
                       RepGroupId specifiedShard,
                       StorageDirectory storageDirectory,
                       LogDirectory logDirectory) {
        this(plan, snId, storageDirectory, logDirectory);
        this.specifiedShard = specifiedShard;
    }

    private DeployNewRN(DeployTopoPlan plan,
                      StorageNodeId snId,
                      StorageDirectory storageDirectory,
                      LogDirectory logDir) {
        super();
        this.plan = plan;
        this.snId = snId;
        if (storageDirectory == null) {
            mountPoint = null;
            storageDirectorySize = 0L;
        } else {
            mountPoint = storageDirectory.getPath();
            storageDirectorySize = storageDirectory.getSize();
        }
        if (logDir == null) {
            logDirectory = null;
            logDirectorySize = 0L;
        } else {
            logDirectory = logDir.getPath();
            logDirectorySize = logDir.getSize();
        }

        /* A more descriptive label used for error messages, etc. */
        StorageNodeParams snp = plan.getAdmin().getStorageNodeParams(snId);
        snDescriptor = snp.displaySNIdAndHost();
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    /**
     * TODO: refactor change port tracker so it generates helper hosts and
     * works on a single SN. Correct now, just would be nicer to share the code.
     */
    private RepNodeParams makeRepNodeParams(Topology current,
                                            RepGroupId rgId,
                                            RepNodeId rnId) {
        /*
         * The RepNodeParams has everything needed to start up the new RepNode.
         */
        final Parameters params = plan.getAdmin().getCurrentParameters();
        final ParameterMap pMap = params.copyPolicies();

        /* Set JE HA host name */
        final String haHostname = params.get(snId).getHAHostname();

        /* Find helper hosts for JE HA */
        final PortTracker portTracker = new PortTracker(current, params, snId);
        final int haPort = portTracker.getNextPort(snId);
        final String otherHelpers = findHelperHosts(current.get(rgId), rnId, params);
        final NodeType nodeType = computeNodeType(current);
        final String helperHosts;
        if (otherHelpers.length() == 0) {
            if (!nodeType.isElectable()) {
                final String msg = "The self-electing node must be electable";
                throw new CommandFaultException(
                    msg, new IllegalStateException(msg),
                    ErrorMessage.NOSQL_5500, CommandResult.NO_CLEANUP_JOBS);
            }
            helperHosts = haHostname + ":" + haPort;
        } else {
            helperHosts = otherHelpers;
        }

        final RepNodeParams rnp =
            new RepNodeParams(pMap, snId, rnId,
                              false /* disabled */,
                              haHostname, haPort, helperHosts,
                              mountPoint, storageDirectorySize,
                              logDirectory, logDirectorySize,
                              nodeType);
        /*
         * If the storage node has a memory setting, set an explicit JE heap
         * and cache size. The new RN has already been added to the topology,
         * so it will be accounted for by current.getHostedRepNodeIds().
         */
        final StorageNodeParams snp = params.get(snId);
        final int numRNsOnSN = current.getHostedRepNodeIds(snId).size();
        final int numANsOnSN = current.getHostedArbNodeIds(snId).size();
        final RNHeapAndCacheSize heapAndCache =
            snp.calculateRNHeapAndCache(pMap, numRNsOnSN,
                                        rnp.getRNCachePercent(), numANsOnSN);
        final long heapMB = heapAndCache.getHeapMB();
        final long cacheBytes = heapAndCache.getCacheBytes();
        final int cachePercent = heapAndCache.getCachePercent();

        /*
         * If the storage node has a num cpus setting, set an explicit
         * -XX:ParallelGCThreads value
         */
        int gcThreads = snp.calcGCThreads();

        plan.getLogger().log
            (Level.INFO,
             "{0} creating {1} on {2} haPort={3}:{4} helpers={5} " +
             "storage directory path={6} directory size={7} " +
             "log directory path ={8} log directory size = {9} " +
             "heapMB={10} cachePercent={11} cacheSize={12} " +
             "-XX:ParallelGCThreads={11}",
             new Object[] {this, rnId, snId, haHostname, haPort, helperHosts,
                           mountPoint, storageDirectorySize,
                           logDirectory, logDirectorySize,
                           (heapMB == 0) ? "unspecified" : heapMB,
                           (cachePercent == 0) ? "unspecified" : cachePercent,
                           (cacheBytes == 0) ? "unspecified" : cacheBytes,
                           (gcThreads == 0) ? "unspecified" : gcThreads});

        /*
         * Set the JVM heap, JE cache, and -XX:ParallelTCThreads.
         */
        rnp.setRNHeapAndJECache(heapAndCache);
        rnp.setParallelGCThreads(gcThreads);

        /* Set the concurrent threads the same as parallel threads */
        rnp.setConcurrentGCThreads(gcThreads);


        /* Duplicates the dns cache ttl values. */
        rnp.duplicateCacheTTLValues(snp.getMap());

        /*
         * Set value of je.rep.node.priority for RN according to the value of
         * master affinity.
         */
        Datacenter dc = current.getDatacenter(snId);

        final boolean masterAffinity = dc.getMasterAffinity();
        if (masterAffinity) {
            rnp.setJENodePriority(MASTER_AFFINITY);
        } else {
            rnp.setJENodePriority(NO_MASTER_AFFINITY);
        }
        return rnp;
    }

    /**
     * Look at the current topology and parameters, as stored in the AdminDB,
     * and generate a set of helpers composed of all the hahost values for the
     * members of the group, other than the target RN.  Returns an empty string
     * if there are no other nodes to use as helpers.  In that case, the caller
     * should use the node's hostname and port to make it self-electing.
     */
    private String findHelperHosts(RepGroup shard,
                                   RepNodeId targetRNId,
                                   Parameters params) {

        final StringBuilder helperHosts = new StringBuilder();

        for (RepNode rn : shard.getRepNodes()) {
            RepNodeId rId = rn.getResourceId();
            if (rId.equals(targetRNId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }

            helperHosts.append(params.get(rId).getJENodeHostPort());
        }
        return helperHosts.toString();
    }

    /** Returns the node type for creating an RN in the specified SN. */
    private NodeType computeNodeType(final Topology current) {
        final Datacenter datacenter = current.getDatacenter(snId);
        return Datacenter.ServerUtil.getDefaultRepNodeType(datacenter);
    }

    @Override
    public State doWork()
        throws Exception {

        /* Create and save a topology and params that represent the new RN. */
        final RepGroupId shardId;
        if (specifiedShard == null) {
            shardId = plan.getShardId(planShardIdx);
        } else {
            shardId = specifiedShard;
        }

        final Admin admin = plan.getAdmin();
        final Topology current = admin.getCurrentTopology();

        final RepGroup rg = current.get(shardId);
        if (rg == null) {
            /*
             * This is really an assert, intended to provide better debugging
             * information than the resulting NPE.
             */
            final String msg = "Expectedly can't find shard " + shardId +
                " current topology=" + TopologyPrinter.printTopology(current);
            throw new CommandFaultException(msg, new IllegalStateException(msg),
                                            ErrorMessage.NOSQL_5500,
                                            CommandResult.NO_CLEANUP_JOBS);
        }
        RepNode rn = null;
        RepNodeParams rnp = null;

        /*
         * If this shard already has a RN on this SN, then this task already
         * executed and this is a retry of the plan. We should use the RN
         * that is there. This assume we will never try to create the two RNs
         * from the same shard on the same SN.
         */
        for (RepNode existing : rg.getRepNodes()) {
            if (existing.getStorageNodeId().equals(snId)) {
                rn = existing;
                rnp = admin.getRepNodeParams(rn.getResourceId());
            }
        }

        if (rn == null) {
            rn = new RepNode(snId);
            rg.add(rn);
            displayRNId = rn.getResourceId();
            rnp =  makeRepNodeParams(current,
                                     rg.getResourceId(),
                                     rn.getResourceId());
            admin.saveTopoAndRNParam(current,
                                     plan.getDeployedInfo(),
                                     rnp, plan);
        } else {
            displayRNId = rn.getResourceId();
        }

        assert TestHookExecute.doHookIfSet(FAULT_HOOK, makeHookTag("1"));

        final Topology topo = admin.getCurrentTopology();
        /*
         * Writes the topology to the topology history system table before the
         * topology is being sent to the new RN in
         * StorageNodeAgentAPI#createRepNode.
         *
         * Writing the topology history usually goes with broadcast of topology.
         * Unlike RelocateRN, however, this task does not broadcast the
         * topology. I think the reason is that during initial deployment, there
         * is not enough RNs for the broadcast task to execute.
         */
        admin.writeTopologyToHistory(topo);

        /*
         * Invoke the creation of the RN after the metadata is safely stored.
         * in the Admin DB.
         */
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils regUtils =
            new RegistryUtils(current, loginMgr, plan.getLogger());
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);

        if (rnp == null) {
            final String msg = "RepNodeParams null for " + rn;
            throw new CommandFaultException(msg, new IllegalStateException(msg),
                                            ErrorMessage.NOSQL_5400,
                                            CommandResult.PLAN_CANCEL);
        }

        try {
            sna.createRepNode(rnp.getMap(),
                              Utils.getMetadataSet(current, plan));
        } catch (IllegalStateException e) {
            throw new CommandFaultException(e.getMessage(), e,
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        /* Register this repNode with the monitor. */
        final StorageNode sn = current.get(snId);
        admin.getMonitor().registerAgent(sn.getHostname(),
                                         sn.getRegistryPort(),
                                         rn.getResourceId());

        /*
         * At this point, we've succeeded. The user will have to rely on ping
         * and on monitoring to wait for the rep node to come up.
         */
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        if (displayRNId == null) {
            return super.getName(sb).append(" on ").append(snDescriptor);
        }
        return super.getName(sb).append(" ").append(displayRNId)
                                .append(" on ").append(snDescriptor);
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
        @Override
        public void run() {
            boolean done = false;
            final Admin admin = plan.getAdmin();
            final Parameters params = admin.getCurrentParameters();
            final long maxRetryTime = ParameterUtils.getDurationMillis(
                params.getPolicies(), ParameterState.AP_NEW_RN_RETRY_TIME);
            final long startTime = System.currentTimeMillis();
            while (!done && !plan.cleanupInterrupted()) {
                try {
                    done = cleanupAllocation();
                } catch (Exception e) {
                    plan.getLogger().log
                        (Level.SEVERE,
                         "{0}: problem when cancelling deployment of RN {1}",
                         new Object[] {this, LoggerUtils.getStackTrace(e)});
                    /*
                     * Don't try to continue with cleanup; a problem has
                     * occurred. Future, additional invocations of the plan
                     * will have to figure out the context and do cleanup.
                     */
                    throw new RuntimeException(e);
                }

                if (!done) {
                    if (System.currentTimeMillis() - startTime
                            > maxRetryTime) {
                        return;
                    }

                    /*
                     * TODO: would be nicer to schedule a job, rather
                     * than sleep.
                     */
                    try {
                        Thread.sleep(AbstractTask.CLEANUP_RETRY_MILLIS);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
        };
    }

    private boolean cleanupAllocation()
        throws RemoteException, NotBoundException {
        final Logger logger = plan.getLogger();

        assert TestHookExecute.doHookIfSet(FAULT_HOOK,
                                           makeHookTag("cleanup"));
        /* RN wasn't created, nothing to do */
        if (displayRNId == null) {
            logger.log(Level.INFO, "{0} cleanup: RN not created.", this);
            return true;
        }

        final Admin admin = plan.getAdmin();
        final TopologyCheck checker =
            new TopologyCheck(this.toString(), logger,
                              admin.getCurrentTopology(),
                              admin.getCurrentParameters());

        final Remedy remedy =
            checker.checkLocation(admin, snId, displayRNId,
                                  true /* calledByDeployNewRN */,
                                  true /* makeRNEnabled */,
                                  null /* oldSNId */,
                                  null /* storageDirectory */);
        logger.log(Level.INFO, "{0} cleanup: {1}", new Object[]{this, remedy});

        return checker.applyRemedy(remedy, plan);
    }

    /**
     * For unit test support -- make a string that uniquely identifies when
     * this task executes on a given SN
     */
    private String makeHookTag(String pointName) {
        return "DeployNewRN/" + snId + "_pt" + pointName;
    }
}
