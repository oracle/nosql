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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams.KrbPrincipalInfo;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.topo.StorageDirectory;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.MasterRepNodeStats;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.pwchecker.PasswordChecker;
import oracle.kv.impl.security.pwchecker.PasswordCheckerFactory;
import oracle.kv.impl.security.pwchecker.PasswordCheckerResult;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.TopologyUtil;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.PingCollector;


/**
 * Utility methods for tasks.
 */
public final class Utils {

    public static volatile TestHook<Topology> beforeBroadcastTopoTestHook;

    private Utils() { /* Prevent construction */ }

    /**
     * Returns the set of metadata required to configure a new RN.
     * @param topo the current topology
     * @param plan the plan
     * @return set of metadata
     */
    static Set<Metadata<? extends MetadataInfo>>
                    getMetadataSet(Topology topo, AbstractPlan plan) {

        if (topo == null) {
            throw new IllegalStateException("Requires non-null topology to " +
                                            "build metadata set");
        }
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                            new HashSet<>();
        metadataSet.add(topo);

        /*
         * In addition to the topology, a new node needs security and table
         * metadata if they have been defined.
         */
        Metadata<? extends MetadataInfo> md = plan.getAdmin().
                                    getMetadata(SecurityMetadata.class,
                                                MetadataType.SECURITY);
        if (md != null) {
            metadataSet.add(md);
        }
        md = plan.getAdmin().getMetadata(TableMetadata.class,
                                         MetadataType.TABLE);
        if (md != null) {
            metadataSet.add(md);
        }
        return metadataSet;
    }

    /**
     * Sends the list of topology changes or the entire new topology to all
     * repNodes.
     *
     * TODO: Remove this method and replace in callers with
     * broadcastMetadataChangesToRNs
     */
    public static boolean broadcastTopoChangesToRNs(Logger logger,
                                                    Topology topo,
                                                    String actionDescription,
                                                    AdminParams params,
                                                    AbstractPlan plan)
        throws InterruptedException {

        return broadcastTopoChangesToRNs(
            logger, topo, actionDescription, params, plan, null,
            Collections.<DatacenterId>emptySet());
    }

    /**
     * Sends the list of topology changes or the entire new topology to all
     * repNodes, optionally skipping RNs in zones that are currently offline.
     *
     * In general, we only send the changes, and not the actual topology.
     * However, if the instigating plan is rerun, we may not be able to create
     * the delta, because we do not know if the broadcast has already
     * executed. In that case, send the whole topology.
     *
     * If there are failures updating repNodes, this method will retry until
     * there is enough successful updates to meet the minimum threshold
     * specified by getBroadcastTopoThreshold(). The threshold is specified
     * as a percent of repNodes. This retry policy is necessary to seed the
     * repNodes with the new topology.
     * @return true if the a topology has been successfully sent to the desired
     * number of nodes, false if the broadcast was stopped due to an interrupt
     * or admin shutting down.
     *
     * TODO - It may be better to do this broadcast in a constrained parallel
     * way, so that the broadcast makes rapid progress even in the presence of
     * some one or a few bad network connections, which may stall on network
     * timeouts. (same for metadata broadcast below)
     *
     * TODO: Remove this method and replace in callers with
     * broadcastMetadataChangesToRNs
     */
    public static boolean broadcastTopoChangesToRNs(
        Logger logger,
        Topology topo,
        String actionDescription,
        AdminParams params,
        AbstractPlan plan,
        RepGroupId failedShard,
        Set<DatacenterId> offlineZones)
        throws InterruptedException {

        assert TestHookExecute.doHookIfSet(beforeBroadcastTopoTestHook, topo);
        /*
         * Write the topology to history before we broadcast.
         *
         * TODO: Currently this is for the purpose of query-under-elasticity so
         * that a query can obtain the topology with a base sequence number.
         * However, writing topology is not necessary when there is no
         * partition migration, e.g., replication factor or capacity change. It
         * seems less important to optimize for no-partition-migration topology
         * changes right now since topology changes are rare in the first
         * place. It might be possible in the future that
         * no-partition-migration topology changes become more frequent and in
         * that case, we will need to optimize for that. One approach for this
         * optimization is that before we broadcast, we do a TopologyDiff
         * between the new and current topology, check the
         * numPartitionMigrations field and do not broadcast if there is no
         * partition migration. On the query side, if it cannot obtain the
         * topology from the history table, it can assume that there is no
         * partition change between the one missing and the last present in the
         * table.
         */
        plan.getAdmin().writeTopologyToHistory(topo);

        logger.log(Level.INFO,
                   "Broadcasting topology seq# {0}" +
                   (!offlineZones.isEmpty() ?
                    ", skipping offline zones {1}" : "") +
                   (failedShard != null ?
                    ", skipping failed shard {2}" : "") +
                   ", changes for {3}",
                   new Object[]{topo.getSequenceNumber(), offlineZones,
                                failedShard, actionDescription});

        final Admin admin = plan.getAdmin();
        final List<RepNodeId> retryList = new ArrayList<>();
        final RegistryUtils registry =
            new RegistryUtils(topo, plan.getLoginManager(), plan.getLogger());
        int nNodes = 0;

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {

            /*
             * Provide functionality to skip sending topo changes
             * to RNs of failed shard
             */
            RepGroupId rgId = rg.getResourceId();
            if (rgId.equals(failedShard)) {
                continue;
            }

            for (RepNode rn : rg.getRepNodes()) {
                if (offlineZones.contains(topo.getDatacenterId(rn))) {
                    continue;
                }
                nNodes++;
                final RepNodeId rnId = rn.getResourceId();

                /* Send the topo. If error, record the RN for retry */
                final int result = sendTopoChangesToRN(logger,
                                                      rnId,
                                                      topo,
                                                      actionDescription,
                                                      registry);
                if (result < 0) {
                    /* No need to broadcast, a newer topo was found */
                    return true;
                } else if (result == 0) {
                    retryList.add(rnId);
                }
            }
        }

        if (retryList.isEmpty()) {
            logger.log(Level.FINE,
                       "Successful broadcast to all nodes of topology " +
                       "seq# {0}" +
                       (!offlineZones.isEmpty() ?
                        ", skipping offline zones {1}" : "") +
                       (failedShard != null ?
                        ", skipping failed shard {2}" : "") +
                       ", for {3}",
                       new Object[]{topo.getSequenceNumber(), offlineZones,
                                    failedShard, actionDescription});

            return true;
        }

        /*
         * The threshold is a percent of existing nodes. The resulting
         * number of nodes must be > 0.
         */
        final int thresholdNodes =
               Math.max((nNodes * params.getBroadcastTopoThreshold()) / 100, 1);
        final int acceptableFailures = nNodes - thresholdNodes;
        final long delay = params.getBroadcastTopoRetryDelayMillis();

        int retries = 0;

        /* Continue to retry until the threshold is met, or interrupted */
        while (retryList.size() > acceptableFailures) {

            final Supplier<String> stopMsg = () -> String.format(
                "stop attempts to broadcast topology changes for %s",
                actionDescription);

            if (plan.isInterruptRequested()) {
                /* stop trying */
                logger.log(Level.INFO,
                           "{0} has been interrupted, {1}",
                           new Object[] {plan, stopMsg.get()});
                return false;
            }

            retries++;

            logger.log(Level.INFO,
                       "Failed to broadcast topology to {0} out of {1} " +
                       "nodes, will retry, acceptable failure threshold={2}, " +
                       "retries={3}",
                       new Object[]{retryList.size(), nNodes,
                                    acceptableFailures, retries});

            if (admin.awaitUntilClosing(delay)) {
                logger.log(Level.INFO,
                           "admin is shutting down during {0}, {1}",
                           new Object[] {plan, stopMsg.get()});
                return false;
            }

            /* Get a new registry in case the failures were network related */
            final RegistryUtils ru =
                new RegistryUtils(topo, plan.getLoginManager(),
                                  plan.getLogger());
            final Iterator<RepNodeId> itr = retryList.iterator();

            while (itr.hasNext()) {
                final int result = sendTopoChangesToRN(logger,
                                                       itr.next(),
                                                       topo,
                                                       actionDescription,
                                                       ru);
                if (result < 0) {
                    /* No need to broadcast, a newer topo was found */
                    return true;
                } else if (result > 0) {
                    itr.remove();
                }
            }
        }

        logger.log(Level.INFO,
                   "Broadcast topology {0} for {1} successful to {2} out of " +
                   "{3} nodes",
                   new Object[]{topo.getSequenceNumber(),
                                actionDescription,
                                nNodes - retryList.size(), nNodes});
        return true;
    }

    /**
     * Sends the list of topology changes or the entire new topology to the
     * specified repNode.
     *
     * @return 1 if the update was successful, 0 if failed, -1 if a newer
     * topology was found
     */
    private static int sendTopoChangesToRN(Logger logger,
                                           RepNodeId rnId,
                                           Topology topo,
                                           String actionDescription,
                                           RegistryUtils registry) {

        final StorageNodeId snId = topo.get(rnId).getStorageNodeId();

        try {
            final RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);
            final int rnTopoSeqNum = rnAdmin.getTopoSeqNum();

            /*
             * Finding the same topology is possible as the RNs will be
             * busy propagating it throughout the store.
             */
            if (rnTopoSeqNum == topo.getSequenceNumber()) {
                return 1;
            }

            /*
             * Finding a newer topology in the wild means some other task
             * has an updated topology and has sent it out.
             */
            if (rnTopoSeqNum > topo.getSequenceNumber()) {
                logger.log(Level.FINE,
                           "{0} has a higher topology sequence number of " +
                           "{1} compared to this topology of {2} while {3}",
                           new Object[]{rnId, rnTopoSeqNum,
                                        topo.getSequenceNumber(),
                                        actionDescription});
                return -1;
            }

            /*
             * If the topology is empty or null, force updating the full topo.
             */
            final List<TopologyChange> changes =
                    (rnTopoSeqNum == Topology.EMPTY_SEQUENCE_NUMBER) ?
                                                null :
                                                topo.getChanges(rnTopoSeqNum);

            if ((changes != null) && (changes.size() > 0)) {
                final int actualTopoSeqNum =
                    rnAdmin.updateMetadata(new TopologyInfo(topo, changes));
                if ((rnTopoSeqNum - actualTopoSeqNum) > 1)  {
                    /*
                     * retry, the target has an older topology than acquired
                     * initially.
                     */
                    logger.log(Level.INFO,
                               "Older topology than expected for {0} on {1}." +
                               " Expected topo seq num: {2} actual: {3}",
                                new Object[]{rnId, snId, rnTopoSeqNum,
                                             actualTopoSeqNum});
                    return 0;
                }
            } else {
                rnAdmin.updateMetadata(topo);
            }
            return 1;
        } catch (RepNodeAdminFaultException rnfe) {
            /*
             * RN had problems with this request; often the problem is that
             * it is not in RUNNING state
             */
            logger.log(Level.INFO,
                      "Unable to update topology for {0} on {1} for {2}" +
                       " during broadcast: {3}",
                       new Object[]{rnId, snId, actionDescription,
                                    rnfe.getMessage()});
        } catch (NotBoundException notbound) {
            logger.log(Level.INFO,
                       "{0} on {1} cannot be contacted for topology update " +
                       "to {2} during broadcast: {3}",
                       new Object[]{rnId, snId, actionDescription, notbound});
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "Could not update topology for {0} on {1} for " +
                       "{2}: {3}",
                       new Object[]{rnId, snId, actionDescription, e});
        }

        /*
         * Any other RuntimeExceptions indicate an unexpected problem, and will
         * fall through and throw out of this method.
         */
        return 0;
    }

    /**
     * Update helper hosts for RN/AN defined by the topology.
     */
    public static boolean updateHelperHost(Admin admin,
                                           Topology topo,
                                           ResourceId resId,
                                           Logger logger) {
        RepGroup rg;
        boolean retStatus;

        if (resId.getType() == ResourceType.REP_NODE) {
            rg = topo.get(topo.get((RepNodeId)resId).getRepGroupId());
            retStatus =
                updateHelperHost(admin, topo, rg, (RepNodeId)resId, logger);
        } else {
            /* Must be an ARB_NODE */
            rg = topo.get(topo.get((ArbNodeId)resId).getRepGroupId());
            retStatus =
                updateHelperHost(admin, topo, rg, (ArbNodeId)resId, logger);
        }
        return retStatus;
    }

    /**
     * Update helper hosts as defined by the rep group.
     */
    public static boolean updateHelperHost(Admin admin,
                                           Topology topo,
                                           RepGroup rg,
                                           RepNodeId rnId,
                                           Logger logger) {

        final RepNodeParams oldRNP = admin.getRepNodeParams(rnId);
        final RepNodeParams newRNP = new RepNodeParams(oldRNP);

        final String updatedHelpers =
            Utils.findHelpers(rnId, admin.getCurrentParameters(), rg);

        /*
         * There are no other helpers available, probably because this is
         * a rep group of 1, so don't change the helper host.
         */
        if (updatedHelpers.length() == 0) {
            return true;
        }

        logger.info("Changing helperHost for " + rnId +
                    " in metadata database " +
                    " to " + updatedHelpers);
        newRNP.setJEHelperHosts(updatedHelpers);
        admin.updateParams(newRNP);
        final StorageNodeId snId = newRNP.getStorageNodeId();

        try {
            /* Ask the SNA to write a new configuration file. */
            final RegistryUtils registryUtils =
                new RegistryUtils(topo, admin.getLoginManager(), logger);
            final StorageNodeAgentAPI sna =
                    registryUtils.getStorageNodeAgent(snId);
            sna.newRepNodeParameters(newRNP.getMap());
        } catch (Exception e) {
            logger.info("Unable to change helperHost for " + rnId +
                        " on " + snId +
                        " to " + updatedHelpers +
                        ". Exception " + e);
            return false;
        }
        logger.info("Changed helperHost for " + rnId +
                    " on " + snId +
                    " to " + updatedHelpers);
        return true;
    }

    public static boolean updateHelperHost(Admin admin,
                                           Topology topo,
                                           RepGroup rg,
                                           ArbNodeId anId,
                                           Logger logger) {

        final ArbNodeParams oldANP = admin.getArbNodeParams(anId);
        final ArbNodeParams newANP = new ArbNodeParams(oldANP);

        final String updatedHelpers =
            Utils.findHelpers(anId, admin.getCurrentParameters(), rg);

        /*
         * There are no other helpers available, probably because this is
         * a rep group of 1, so don't change the helper host.
         */
        if (updatedHelpers.length() == 0) {
            return true;
        }


        logger.info("Changing helperHost for " + anId +
                    " in metadata database " +
                    " to " + updatedHelpers);
        newANP.setJEHelperHosts(updatedHelpers);
        admin.updateParams(newANP);
        final StorageNodeId snId = newANP.getStorageNodeId();

        try {
            /* Ask the SNA to write a new configuration file. */
            final RegistryUtils registryUtils =
                new RegistryUtils(topo, admin.getLoginManager(), logger);
            final StorageNodeAgentAPI sna =
                    registryUtils.getStorageNodeAgent(snId);
            sna.newArbNodeParameters(newANP.getMap());
        } catch (Exception e) {
            logger.info("Unable to change helperHost for " + anId +
                        " on " + snId +
                        " to " + updatedHelpers +
                        ". Exception " + e);
            return false;
        }
        logger.info("Changed helperHost for " + anId +
                    " on " + snId +
                    " to " + updatedHelpers);
        return true;
    }

    /**
     * Generate helper hosts for RN/AN as defined by the topology.
     */
    public static String findHelpers(ResourceId resId,
                                     Parameters params,
                                     Topology topo) {
        final RepGroup rg;
        if (resId.getType() == ResourceType.REP_NODE) {
            rg = topo.get(topo.get((RepNodeId)resId).getRepGroupId());
        } else {
            rg = topo.get(topo.get((ArbNodeId)resId).getRepGroupId());
        }
        return findHelpers(resId, params, rg);
    }

    /**
     * Generate the most complete set of helper hosts possible by appending all
     * the nodeHostPort values for all other members of this HA repGroup. If
     * the node is an RN and the only member of the group, returns its own host
     * and port.
     */
    public static String findHelpers(ResourceId targetId,
                                     Parameters params,
                                     RepGroup rg) {

        final StringBuilder helperHosts = new StringBuilder();
        String thisRNHelper = null;
        for (RepNode rn : rg.getRepNodes()) {
            final RepNodeId rid = rn.getResourceId();
            final RepNodeParams peerParams = params.get(rid);
            final String helper = peerParams.getJENodeHostPort();
            if (rid.equals(targetId)) {
                thisRNHelper = helper;
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
            }
            helperHosts.append(helper);
        }

        for (ArbNode an : rg.getArbNodes()) {
            final ArbNodeId aid = an.getResourceId();

            if (aid.equals(targetId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
            }

            final ArbNodeParams peerParams = params.get(aid);
            helperHosts.append(peerParams.getJENodeHostPort());
        }

        if ((helperHosts.length() == 0) && (thisRNHelper != null)) {
            return thisRNHelper;
        }

        return helperHosts.toString();
    }

    static boolean updateAdminHelpersAndNotify(Admin admin,
                                               Parameters p,
                                               AbstractPlan plan) {
        boolean success = true;
        for (AdminParams ap : p.getAdminParams()) {
            final String helpers = findAdminHelpers(p, ap.getAdminId());
            if (helpers.length() == 0) {
                continue;
            }
            final AdminParams newAp = new AdminParams(ap.getMap().copy());

            newAp.setHelperHost(helpers);
            admin.updateParams(newAp);
            plan.getLogger().log(Level.INFO, "{0} changed helperHost to {1}",
                                 new Object[]{p, helpers});

            /* Tell the SNA about it. */
            final StorageNodeId snid = newAp.getStorageNodeId();
            try {
                final StorageNodeAgentAPI sna =
                    RegistryUtils.getStorageNodeAgent(p, snid,
                                                      plan.getLoginManager(),
                                                      plan.getLogger());
                sna.newAdminParameters(newAp.getMap());
            } catch (Exception e) {
                success = false;
                plan.getLogger().log(Level.INFO, "{0} unable to notify SNA " +
                                     "{1} of Admin helper hosts change.",
                                     new Object[]{p, snid});
                continue;
            }
            /* notify admin of new parameters */
            StorageNodeParams snp = p.get(snid);
            String hostname = snp.getHostname();
            int registryPort = snp.getRegistryPort();
            try {
                final CommandServiceAPI cs = ServiceUtils.waitForAdmin(
                    hostname, registryPort, plan.getLoginManager(),
                    40, ServiceStatus.RUNNING, plan.getLogger());

                 cs.newParameters();
            } catch (Exception e) {
                success = false;
                plan.getLogger().log(Level.INFO, "{0} unable to notify Admin " +
                                     "{1} of Admin helper hosts change.",
                                     new Object[]{p, ap.getAdminId()});
                continue;
            }
        }
        return success;
    }

    static String findAdminHelpers(Parameters p,
                                   AdminId target) {
        final StringBuilder helperHosts = new StringBuilder();
        if (p.getAdminCount() == 1) {
            /* If there is only one Admin, it is its own helper. */
            helperHosts.append(p.get(target).getNodeHostPort());
        } else {
            for (AdminParams ap : p.getAdminParams()) {
                final AdminId aid = ap.getAdminId();
                if (aid.equals(target)) {
                    continue;
                }
                if (helperHosts.length() != 0) {
                    helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
                }
                helperHosts.append(ap.getNodeHostPort());
            }
        }
        return helperHosts.toString();
    }

    /**
     * @throws OperationFaultException if the HA address could not be changed.
     */
    static void changeHAAddress(Topology topo,
                                Parameters parameters,
                                AdminParams adminParams,
                                ResourceId rId,
                                StorageNodeId oldNode,
                                StorageNodeId newNode,
                                AbstractPlan plan)
        throws InterruptedException {

        RepGroup rg;
        String targetNodeHostPort;
        String targetHelperHosts;
        String groupName;

        if (rId instanceof RepNodeId) {
            final RepNodeId rnId = (RepNodeId)rId;
            rg = topo.get(topo.get(rnId).getRepGroupId());
            targetNodeHostPort = parameters.get(rnId).getJENodeHostPort();
            targetHelperHosts = parameters.get(rnId).getJEHelperHosts();
            groupName = rnId.getGroupName();

            /*
             * Only need to change the HA address if both the old
             * and new SNs are in primary data centers, since information
             * about secondary nodes is not recorded persistently in the
             * replication group. It should be noted that an AN has persistent
             * information in the JEHA database. An AN may be in a
             * secondary zone if the zone is offlined (converted from a
             * primary to a secondary zone). This is why there is no
             * check like this in the case of an AN.
             */
            final Datacenter oldDC = topo.getDatacenter(oldNode);
            final Datacenter newDC = topo.getDatacenter(newNode);
            if (!(oldDC.getDatacenterType().isPrimary() &&
                  newDC.getDatacenterType().isPrimary())) {
                return;
            }

        } else {
            final ArbNodeId anId = (ArbNodeId)rId;
            rg = topo.get(topo.get(anId).getRepGroupId());
            targetNodeHostPort = parameters.get(anId).getJENodeHostPort();
            targetHelperHosts = parameters.get(anId).getJEHelperHosts();
            groupName = anId.getGroupName();
        }

        /*
         * Find the first node that is not the target node, and ask it
         * to update addresses. If it can't, continue trying other
         * members of the group. If the master is not available,
         * continue trying for some time.
         */
        boolean done = false;

        final long delay = adminParams.getBroadcastTopoRetryDelayMillis();
        final Logger logger = plan.getLogger();

        logger.log(Level.INFO,
                   "{0} change haPort for {0} to relocate from {1} to {2}",
                   new Object[]{plan, rId, oldNode, newNode});
        while (!done && !plan.isInterruptRequested()) {

            boolean groupHasNoMaster = false;

            /* Try each RN in turn. Only one has to get the update out. */
            for (RepNode rn : rg.getRepNodes()) {
                final RepNodeId peerId = rn.getResourceId();
                if (peerId.equals(rId)) {
                    continue;
                }

                /* Found a peer repNode */
                try {
                    final RegistryUtils registry =
                        new RegistryUtils(topo, plan.getLoginManager(),
                                          logger);
                    final RepNodeAdminAPI rnAdmin =
                            registry.getRepNodeAdmin(peerId);
                    if (rnAdmin.updateMemberHAAddress(groupName,
                                                      rId.getFullName(),
                                                      targetHelperHosts,
                                                      targetNodeHostPort)) {
                        /*
                         * One of the RNs or ANs was able to get the update out
                         * successfully.
                         */
                        done = true;
                        break;
                    }
                    logger.log(Level.INFO,
                               "{0} {1} attempted to update HA address for " +
                               "{2} while relocating from {3} to {4}  but " +
                               "shard has no master",
                               new Object[]{plan, peerId, rId,
                                            oldNode, newNode});
                    groupHasNoMaster = true;
                } catch (RepNodeAdminFaultException e) {
                    logger.log(Level.SEVERE,
                        "{0} {1} experienced an exception when attempting to" +
                        " update HA address for {2} while relocating from" +
                        " {3} to {4}: {5}",
                         new Object[] {plan, peerId, rId,
                                       oldNode, newNode, e.getMessage() });
                } catch (NotBoundException e) {
                    logger.log(Level.SEVERE,
                            "{0} {1} could not be contacted, experienced an" +
                            " exception when attempting to update HA address" +
                            " for {2} while relocating from {3} to {4}: {5}",
                            new Object[] {plan, peerId, rId,
                                          oldNode, newNode, e });
                } catch (RemoteException e) {
                    logger.log(Level.SEVERE,
                            "{0} {1} could not be contacted, experienced an" +
                            " exception when attempting to update HA address" +
                            " for {2} while relocating from {3} to {4}: {5}",
                            new Object[] {plan, peerId, rId,
                                          oldNode, newNode, e });
                }
            }

            /* Someone was able to get the update out successfully */
            if (done) {
                return;
            }

            /*
             * No-one in the group was able to do the update. Retry only if the
             * group has no master, and we are waiting for a new master to be
             * elected. Wait before retrying. TODO: this uses the same pattern
             * as broadcastTopoChangesToRNs, in that it sleeps and is
             * susceptible to interrupt. Ideally, we make the plan interrupt
             * flag available to implement a softer interrupt. Should also
             * consider moving this retry loop to a higher level, so retries
             * are implemented by the task, as multiple phases. Not strictly
             * necessary if this is only called by serial tasks, because there
             * is no concern with tying up a planner thread.
             */
            if (groupHasNoMaster) {
                /*
                 * We only retry if we have positive info that there was no
                 * master and that a retry should work soon.
                 */
                logger.log(Level.INFO,
                           "{0} no master for shard while updating HA address" +
                           " for {1} from {2} to {3}. Wait and retry",
                           new Object[]{plan, rId, oldNode, newNode});
                Thread.sleep(delay);
            } else {
                /*
                 * Unexpected problems, couldn't contact anyone in group,
                 * give up.
                 */
                logger.log(Level.INFO,
                           "{0} could not contact any member of the shard " +
                           "while updating HA address for {1} from {2} to {3}" +
                           ". Give up.",
                           new Object[]{plan, rId, oldNode, newNode});
                break;
            }
        }

        if (!done) {
            throw new OperationFaultException
                ("Couldn't change HA address for " + rId + " to " +
                 targetNodeHostPort + " while migrating " + oldNode + " to " +
                 newNode);
        }
    }

    public static void removeHAAddress(Topology topo,
                                       AdminParams adminParams,
                                       ResourceId rId,
                                       StorageNodeId oldNode,
                                       AbstractPlan plan,
                                       RepGroupId rgId,
                                       String targetHelperHosts,
                                       Logger logger)
        throws InterruptedException {

        final RepGroup rg = topo.get(rgId);

        /*
         * Find the first node that is not the target node, and ask it
         * to update addresses. If it can't, continue trying other
         * members of the group. If the master is not available,
         * continue trying for some time.
         */
        final long delay = adminParams.getBroadcastTopoRetryDelayMillis();

        logger.log(Level.INFO,
                   "{0} remove ha entry for {1} on SN {2}}",
                   new Object[]{plan, rId, oldNode});
        while (!plan.isInterruptRequested()) {

            boolean groupHasNoMaster = false;

            /* Try each RN in turn. Only one has to get the update out. */
            for (RepNode rn : rg.getRepNodes()) {
                final RepNodeId peerId = rn.getResourceId();
                if (peerId.equals(rId)) {
                    continue;
                }

                /* Found a peer repNode */
                try {
                    final RegistryUtils registry =
                        new RegistryUtils(topo, plan.getLoginManager(),
                                          logger);
                    final RepNodeAdminAPI rnAdmin =
                            registry.getRepNodeAdmin(peerId);

                    if (rnAdmin.deleteMember(rgId.getGroupName(),
                                             rId.getFullName(),
                                             targetHelperHosts)) {
                        return;
                    }
                    logger.log(Level.INFO,
                               "{0} attempting to remove HA address for {1} " +
                               "from {2} but shard has no " +
                               "master. Wait and retry",
                                new Object[]{plan, rId, oldNode});
                    groupHasNoMaster = true;
                } catch (RepNodeAdminFaultException e) {
                    logger.log(Level.SEVERE,
                               "{0} {1} experienced an exception when " +
                               "attempting to remove HA address for {2} from " +
                               "{3}: {4}",
                               new Object[] {plan, peerId, rId, oldNode,
                                             e.getMessage() });
                } catch (NotBoundException e) {
                    logger.log(Level.SEVERE,
                               "{0} {1} could not be contacted, experienced " +
                               "an exception when attempting to remove HA " +
                               "address for {2} from {3}: {4}",
                               new Object[] {plan, peerId, rId, oldNode, e });
                } catch (RemoteException e) {
                    logger.log(Level.SEVERE,
                               "{0} {1} could not be contacted, experienced " +
                               "an exception when attempting to remove HA " +
                               "address for {2} from {3}: {4}",
                               new Object[] {plan, peerId, rId, oldNode, e });
                }
            }

            /*
             * No-one in the group was able to do the update. Retry only if the
             * group has no master, and we are waiting for a new master to be
             * elected. Wait before retrying. TODO: this uses the same pattern
             * as broadcastTopoChangesToRNs, in that it sleeps and is
             * susceptible to interrupt. Ideally, we make the plan interrupt
             * flag available to implement a softer interrupt. Should also
             * consider moving this retry loop to a higher level, so retries
             * are implemented by the task, as multiple phases. Not strictly
             * necessary if this is only called by serial tasks, because there
             * is no concern with tying up a planner thread.
             */
            if (groupHasNoMaster) {
                /*
                 * We only retry if we have positive info that there was no
                 * master and that a retry should work soon.
                 */
                logger.log(Level.INFO,
                           "{0} no master for shard while remove HA address " +
                           "for {1} from {2}. Wait and retry",
                           new Object[]{plan, rId, oldNode});
                Thread.sleep(delay);
            } else {
                /*
                 * Unexpected problems, couldn't contact anyone in group,
                 * give up.
                 */
                logger.log(Level.INFO,
                           "{0} could not contact any member of the shard " +
                           "while removing HA address for {1} from {2}." +
                           " Give up.",
                           new Object[]{plan, rId, oldNode});
                break;
            }
        }

        throw new OperationFaultException
            ("Couldn't remove HA address for " + rId +
             "from " + oldNode);
    }

    /**
     * Stops the specified RN. If awaitConsistent is true and the target RN
     * is a primary node, before shutting down, this method will wait for
     * enough RNs in the target RN's group to become consistent to
     * maintain quorum.
     *
     * Do not wait when shutting down the store as this will eventually
     * fail when the number of running nodes drops below quorum.
     *
     * If awaitConsistent is true, this method can throw an
     * OperationFaultException if the wait times out (if the target node
     * is a primary) or the wait is interrupted.
     *
     * @param plan the enclosing plan
     * @param snId the SN hosting the target RN
     * @param rnId the target RN to stop
     * @param awaitConsistent if true waits for relica(s) to catch up
     * @param failedShard if true just update admin params for stopped
     * victim
     *
     * @throws OperationFaultException if the HA address could not be changed.
     */
    static void stopRN(AbstractPlan plan,
                       StorageNodeId snId,
                       RepNodeId rnId,
                       boolean awaitConsistent,
                       boolean failedShard)
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();

        if (awaitConsistent) {
            HealthCheck.create(plan.getAdmin(), plan.toString(), rnId).await();
        }

        plan.getLogger().log(Level.INFO, "{0} stopping {1} on {2}",
                             new Object[]{plan, rnId, snId});
        /*
         * Update the rep node params to indicate that this node is now
         * disabled, and save the changes.
         */
        final RepNodeParams rnp =
            new RepNodeParams(admin.getRepNodeParams(rnId));
        final boolean oldDisableFlag = rnp.isDisabled();
        rnp.setDisabled(true);
        admin.updateParams(rnp);

        // TODO: ideally, put a call in to force a collection of monitored
        // data, to update monitoring before stopping this node.

        /* Tell the SNA to stop the node.
         * If failedShard is specified then no need to stop node,
         * it is already stopped because of failures. Stopping node
         * in such case will result in failure.
         */
        if (!failedShard) {
            final Topology topology = admin.getCurrentTopology();
            final RegistryUtils registryUtils =
                new RegistryUtils(topology, admin.getLoginManager(),
                                  plan.getLogger());
            try {
                final StorageNodeAgentAPI sna =
                    registryUtils.getStorageNodeAgent(snId);
                sna.stopRepNode(rnId, false, true /* checkStream */,
                                plan.toString());
            } catch (Exception e) {
                if (!oldDisableFlag) {
                    rnp.setDisabled(false);
                    admin.updateParams(rnp);
                }
                throw e;
            }
        }

        /* Stop monitoring this node. */
        admin.getMonitor().unregisterAgent(rnId);

        /*
         * Ask the monitor to collect status now for this rep node, so that it
         * will realize that it has been disabled, and this changed status
         * will display sooner.
         */
        admin.getMonitor().collectNow(snId);
    }

    public static void disableAndStopAN(AbstractPlan plan,
                                        StorageNodeId snId,
                                        ArbNodeId anId)
        throws RemoteException, NotBoundException {


        plan.getLogger().log(Level.INFO, "{0} disabling {1} on {2}",
                             new Object[]{plan, anId, snId});

        /*
        *
        * Update the arb node params to indicate that this node is now
        * disabled, and save the changes.
        */
        final Admin admin = plan.getAdmin();
        final ArbNodeParams anp =
            new ArbNodeParams(admin.getArbNodeParams(anId));
        final boolean oldDisabledValue = anp.isDisabled();
        anp.setDisabled(true);
        admin.updateParams(anp);
        try {
            stopAN(plan, snId, anId);
        } catch (Exception e) {
            if (!oldDisabledValue) {
                anp.setDisabled(false);
                admin.updateParams(anp);
            }
            throw e;
        }
    }

    public static void disableAN(AbstractPlan plan,
                                 StorageNodeId snId,
                                 ArbNodeId anId) {

        plan.getLogger().log(Level.INFO, "{0} disabling {1} on {2}",
                             new Object[]{plan, anId, snId});

        /*
        *
        * Update the arb node params to indicate that this node is now
        * disabled, and save the changes.
        */
        final Admin admin = plan.getAdmin();
        final ArbNodeParams anp =
            new ArbNodeParams(admin.getArbNodeParams(anId));
        anp.setDisabled(true);
        admin.updateParams(anp);
    }

    public static void stopAN(AbstractPlan plan,
                              StorageNodeId snId,
                              ArbNodeId anId)
        throws RemoteException, NotBoundException {

        plan.getLogger().log(Level.INFO, "{0} stopping {1} on {2}",
                             new Object[]{plan, anId, snId});

        final Admin admin = plan.getAdmin();

        // TODO: ideally, put a call in to force a collection of monitored
        // data, to update monitoring before stopping this node.

        /* Tell the SNA to stop the node. */
        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(),
                              plan.getLogger());
        final StorageNodeAgentAPI sna =
                registryUtils.getStorageNodeAgent(snId);
        sna.stopArbNode(anId, false, plan.toString());

        /* Stop monitoring this node. */
        admin.getMonitor().unregisterAgent(anId);

        /*
         * Ask the monitor to collect status now for this rep node, so that it
         * will realize that it has been disabled, and this changed status
         * will display sooner.
         */
        admin.getMonitor().collectNow(snId);
    }

    static void startRN(AbstractPlan plan,
                        StorageNodeId snId,
                        RepNodeId rnId)
        throws RemoteException, NotBoundException {

        plan.getLogger().log(Level.INFO, "{0} starting {1} on {2}",
                             new Object[]{plan, rnId, snId});

        /*
         * Check the topology to make sure that the RepNode exists, in
         * case the topology was changed after the plan was constructed, and
         * before it ran. TODO: this can't actually happen yet because we
         * don't support the removal of RepNodes. Test when store contraction
         * is supported in later releases.
         */
        final Admin admin = plan.getAdmin();
        final Topology topology = admin.getCurrentTopology();
        final RepNode rn = topology.get(rnId);
        if (rn == null) {
            throw new IllegalCommandException
                (rnId +
                 " was removed from the topology and can't be started");
        }

        /*
         * Update the rep node params to indicate that this node is now enabled,
         * and save the changes.
         */
        final RepNodeParams rnp =
            new RepNodeParams(admin.getRepNodeParams(rnId));
        rnp.setDisabled(false);
        admin.updateParams(rnp);

        /* Tell the SNA to startup the node. */
        final AdminServiceParams asp = admin.getParams();
        final StorageNodeParams snp = admin.getStorageNodeParams(snId);
        final String storeName = asp.getGlobalParams().getKVStoreName();
        final StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent(
                storeName,
                snp.getHostname(),
                snp.getRegistryPort(),
                snId,
                admin.getLoginManager(),
                plan.getLogger());
        sna.startRepNode(rnId);

        /*
         * Check if the RN experienced a problem directly at startup time.
         */
        RegistryUtils.checkForStartupProblem(storeName, snp.getHostname(),
                                             snp.getRegistryPort(), rnId, snId,
                                             admin.getLoginManager(),
                                             plan.getLogger());

        /*
         * Tell the Monitor to start monitoring this node. Registering
         * an agent is idempotent
         */
        final StorageNode sn = topology.get(snId);
        plan.getAdmin().getMonitor().registerAgent(sn.getHostname(),
                                                   sn.getRegistryPort(),
                                                   rnId);

    }

    public static void startAN(AbstractPlan plan,
                               StorageNodeId snId,
                               ArbNodeId anId)
        throws RemoteException, NotBoundException {

        plan.getLogger().log(Level.INFO, "Starting {0} on {1}",
                             new Object[]{anId, snId});

        /*
         * Check the topology to make sure that the RepNode exists, in
         * case the topology was changed after the plan was constructed, and
         * before it ran. TODO: this can't actually happen yet because we
         * don't support the removal of RepNodes. Test when store contraction
         * is supported in later releases.
         */
        final Admin admin = plan.getAdmin();
        final Topology topology = admin.getCurrentTopology();
        final ArbNode an = topology.get(anId);
        if (an == null) {
            throw new IllegalCommandException
                (anId +
                 " was removed from the topology and can't be started");
        }

        /*
         * Update the arb node params to indicate that this node is now enabled,
         * and save the changes.
         */
        final ArbNodeParams anp =
            new ArbNodeParams(admin.getArbNodeParams(anId));
        anp.setDisabled(false);
        admin.updateParams(anp);

        /* Tell the SNA to startup the node. */
        final AdminServiceParams asp = admin.getParams();
        final StorageNodeParams snp = admin.getStorageNodeParams(snId);
        final String storeName = asp.getGlobalParams().getKVStoreName();
        final StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent(storeName,
                                              snp.getHostname(),
                                              snp.getRegistryPort(),
                                              snId,
                                              admin.getLoginManager(),
                                              plan.getLogger());
        sna.startArbNode(anId);

        /*
         * Check if the AN experienced a problem directly at startup time.
         */
        RegistryUtils.checkForStartupProblem(storeName, snp.getHostname(),
                                             snp.getRegistryPort(), anId, snId,
                                             admin.getLoginManager(),
                                             plan.getLogger());

        /*
         * Tell the Monitor to start monitoring this node. Registering
         * an agent is idempotent
         */
        final StorageNode sn = topology.get(snId);
        plan.getAdmin().getMonitor().registerAgent(sn.getHostname(),
                                                   sn.getRegistryPort(),
                                                   anId);
    }

    /**
     * Waits for specified RN to catch up to the master. If the target RN is
     * the master, the methods returns without waiting. Throws
     * OperationFaultException if the wait is interrupted.
     *
     * @param plan current plan
     * @param rnId the target RN
     *
     * @throws OperationFaultException on interrupt
     */
    static void awaitConsistent(AbstractPlan plan, RepNodeId rnId) {

        /* Max and min time between polling the RN */
        final long MAX_POLLING_SECS = 10;
        final long MIN_POLLING_SECS = 2;

        final String targetName = rnId.getFullName();
        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        final AdminParams ap = admin.getParams().getAdminParams();

        /* A node is considered caught-up if at or below this threshold */
        final long thresholdMillis = ap.getAckTimeoutMillis();

        final long timeoutMillis =
                        ap.getWaitTimeoutUnit().toMillis(ap.getWaitTimeout());

        plan.getLogger().log(Level.FINE,
                             "Waiting up to {0} ms for {1} to " +
                             "become consistent, threshold= {2} ms",
                            new Object[]{timeoutMillis, rnId, thresholdMillis});

        final RepGroupId rgId = new RepGroupId(rnId.getGroupId());

        final long limitMillis = System.currentTimeMillis() + timeoutMillis;
        final PingCollector collector =
            new PingCollector(topo, plan.getLogger());
        while (true) {
            final Map<RepNodeId, RepNodeStatus> statusMap =
                collector.getRepNodeStatus(rgId);

            MasterRepNodeStats stats = null;

            /* Find the master's stats, if the target is the master exit */
            for (Map.Entry<RepNodeId, RepNodeStatus> e : statusMap.entrySet()) {
                final RepNodeStatus status = e.getValue();

                if ((status != null) &&
                    status.getReplicationState().isMaster()) {
                    if (e.getKey().equals(rnId)) {
                        return;
                    }
                    stats = status.getMasterRepNodeStats();
                    break;
                }
            }

             /* Wait the min time if we didn't find stats */
            long waitSecs = MIN_POLLING_SECS;

            if (stats != null) {
                final Map<String, Long> delayMap =
                        stats.getReplicaDelayMillisMap();
                final Long msBehind = delayMap.get(targetName);

                if ((msBehind != null) && (msBehind <= thresholdMillis)) {
                    return;
                }

                /*
                 * If available, use the catchup time to wait if it positive
                 * but less than the maximum
                 */
                final Long catchupSecs =
                                stats.getReplicaCatchupTimeSecs(targetName);
                if ((catchupSecs != null) && (catchupSecs > 0)) {
                    waitSecs = catchupSecs;
                }
            }
            final long waitMillis = SECONDS.toMillis(
                              (waitSecs > MAX_POLLING_SECS) ? MAX_POLLING_SECS :
                                                              waitSecs);
            if ((System.currentTimeMillis() + waitMillis) > limitMillis) {

                /* Don't fail if waiting times out */
                plan.getLogger().log(Level.FINE,
                                     "Waiting for {0} to reach consistency " +
                                     "timed-out", rnId);
                return;
            }
            plan.getLogger().log(Level.FINE,
                                 "Waiting {0} seconds for {1} to " +
                                 "reach consistency",
                                 new Object[]{waitSecs, rnId});
            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException ie) {
                throw new OperationFaultException(
                                    "Unexpected interrupt while waiting for " +
                                    rnId + " to reach consistency");
            }
        }
    }

    /**
     * Gets the number of electable nodes in the specified replication group
     * based on the specified parameters.
     */
    static int getElectableRF(Parameters dbParams, int rgId) {
        int electableRF = 0;
        for (RepNodeParams rnp : dbParams.getRepNodeParams()) {
            if ((rnp.getRepNodeId().getGroupId() == rgId) &&
                rnp.getNodeType().isElectable()) {
                electableRF++;
            }
        }
        return electableRF;
    }

    /**
     *  Wait for RepNode or ArbNode to be in the target state.
     *
     * @param plan
     * @param resId
     * @param targetState
     * @return STATE.ERROR if target state did not occur within the timeout,
     * otherwise return STATE.SUCCEEDED
     * @throws InterruptedException
     */
    public static Task.State waitForNodeState(AbstractPlan plan,
                                              ResourceId resId,
                                              ServiceStatus targetState)
        throws InterruptedException {

        final AdminServiceParams asp = plan.getAdmin().getParams();
        final AdminParams ap = asp.getAdminParams();
        final long waitSeconds =
            ap.getWaitTimeoutUnit().toSeconds(ap.getWaitTimeout());

        final String msg = "Waiting " + waitSeconds + " seconds for Node " +
            resId + " to reach " + targetState;

        plan.getLogger().fine(msg);

        final StorageNodeId snId;
        if (resId instanceof RepNodeId) {
            RepNodeParams rnp =
                plan.getAdmin().getRepNodeParams((RepNodeId)resId);

            /*
             * Since other, earlier tasks may have failed, it's possible that we
             * may be trying to wait for an nonexistent rep node.
             */
            if (rnp == null) {
                throw new OperationFaultException
                    (msg + ", but that that RepNode " +
                    "doesn't exist in the store");
            }
            snId = rnp.getStorageNodeId();
        } else {
            /* must be an arb node */
            final ArbNodeParams anp =
                plan.getAdmin().getArbNodeParams((ArbNodeId)resId);
            if (anp == null) {
                throw new OperationFaultException
                (msg + ", but that that Node " + resId +
                " doesn't exist in the store");
            }
            snId = anp.getStorageNodeId();
        }

        final StorageNodeParams snp =
            plan.getAdmin().getStorageNodeParams(snId);

        final String storename = asp.getGlobalParams().getKVStoreName();
        final String hostname = snp.getHostname();
        final int regPort = snp.getRegistryPort();
        final LoginManager loginMgr = plan.getLoginManager();

        try {
            final ServiceStatus[] target = {targetState};
            if (resId instanceof RepNodeId) {
                ServiceUtils.waitForRepNodeAdmin(storename,
                                                 hostname,
                                                 regPort,
                                                 (RepNodeId)resId,
                                                 snId,
                                                 loginMgr,
                                                 waitSeconds,
                                                 target,
                                                 plan.getLogger());
            } else {
                ServiceUtils.waitForArbNodeAdmin(storename,
                                                 hostname,
                                                 regPort,
                                                 (ArbNodeId)resId,
                                                 snId,
                                                 loginMgr,
                                                 waitSeconds,
                                                 target,
                                                 plan.getLogger());
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            plan.getLogger().log(Level.INFO, "{0} timed out while {1}",
                                 new Object[]{plan, msg});
            RegistryUtils.checkForStartupProblem(storename,
                                                 hostname,
                                                 regPort,
                                                 resId,
                                                 snId,
                                                 loginMgr,
                                                 plan.getLogger());

            return Task.State.ERROR;
        }

        /*
         * Ask the monitor to collect status now for this rep node,so a
         * new status will be available sooner
         */
        plan.getAdmin().getMonitor().collectNow(resId);
        return Task.State.SUCCEEDED;

    }

    /**
     * Confirms the status of the specified SN. If shouldBeRunning is true
     * the SN must be up and have the status of RUNNING, otherwise
     * OperationFaultException is thrown. If shouldBeRunning is false, an
     * OperationFaultException is thrown only if the SN can be contacted and has
     * RUNNING status. Note that if shouldBeRunning is false this method is not
     * exact as it will pass a node which cannot be contacted.
     *
     * @param topology the current topology
     * @param snId the SN to check
     * @param shouldBeRunning true if the SN must have RUNNING status
     * @param infoMsg the message to include with the exception
     *
     * @throws OperationFaultException if the status of the SN does not meet
     *         the requirement
     */
    static void confirmSNStatus(Topology topology,
                                LoginManager loginMgr,
                                Logger logger,
                                StorageNodeId snId,
                                boolean shouldBeRunning,
                                String infoMsg) {

        /* Check if this storage node is already running. */
        final RegistryUtils registry =
            new RegistryUtils(topology, loginMgr, logger);

        try {
            final StorageNodeAgentAPI sna = registry.getStorageNodeAgent(snId);
            final ServiceStatus serviceStatus = sna.ping().getServiceStatus();

            if (shouldBeRunning) {
                if (serviceStatus == ServiceStatus.RUNNING) {
                    return;
                }
            } else {
                if (!serviceStatus.isAlive()) {
                    return;
                }
            }

            throw new OperationFaultException
                (snId + " has status " + serviceStatus + ". " + infoMsg);
        } catch (NotBoundException | RemoteException notbound) {
            if (shouldBeRunning) {
                throw new OperationFaultException
                    (snId + " cannot be contacted." + infoMsg);
            }
            /* Ok for this node to be unreachable */
        }
    }

    /**
     * Sends the list of metadata changes or the entire metadata to all
     * groups. The broadcast is made to the master of the group unless the
     * metadata is of type TOPOLOGY. In that case the metadata is sent to
     * all the nodes in the group.
     *
     * If there are failures updating repNodes, this method will retry until
     * there are enough successful updates to meet the minimum threshold
     * specified by getBroadcastTopoThreshold(). The threshold is specified
     * as a percent of groups. This retry policy is necessary to seed the
     * groups with the new metadata.
     *
     * TODO - It may be better to do this broadcast in a constrained parallel
     * way, so that the broadcast makes rapid progress even in the presence of
     * some one or a few bad network connections, which may stall on network
     * timeouts.
     *
     * TODO - The code implements "retry until interrupted or we have enough
     * coverage". It may be worthwhile changing that to "retry until
     * interrupted, or we have enough coverage and the last N retries made
     * no progress". Seems like ensuring at least a minimal level of retry
     * might be worthwhile.
     *
     * @param logger a logger
     * @param md a metadata object to broadcast to RNs
     * @param topo the current store topology
     * @param actionDescription description included in logging
     * @param params admin params
     * @param plan the current executing plan
     *
     * @return true if the a metadata has been successfully sent to the desired
     * number of groups or a newer metadata has been encountered, false if the
     * broadcast was stopped due to an interrupt.
     */
    @SuppressWarnings("unchecked")
    static boolean broadcastMetadataChangesToRNs(Logger logger,
                                                 Metadata<?> md,
                                                 Topology topo,
                                                 String actionDescription,
                                                 AdminParams params,
                                                 AbstractPlan plan) {
        /*
         * Unlike the other metadata types, topology is kept in a non-replicated
         * DB on the RN. So it is useful to attempt to send it to all of the
         * nodes in the group, not just the master.
         */
        final boolean masterOnly = !md.getType().equals(MetadataType.TOPOLOGY);

        final List<RepGroup> retryList;
        if (masterOnly) {
            /*
             * When broadcasting to masters only we use a fixed, known order
             * so that we can coordinate with reads of the metadata from
             * the RNs.
             */
            retryList = TopologyUtil.getOrderedRepGroups(topo);
        } else {
            retryList =  new ArrayList<>(topo.getRepGroupMap().getAll());
            /*
             * Shuffle the RN list to help prevent competing broadcast threads
             * from chasing each other.
             */
            Collections.shuffle(retryList);
        }

        final int nGroups = retryList.size();

        if (nGroups < 1) {
            logger.log(Level.FINE,
                       "{0} attempting to broadcast {1} to an empty store " +
                       "for {2}",
                       new Object[] {plan, md.getType(), actionDescription});
            return true;
        }

        logger.log(Level.INFO, "{0} broadcasting {1} for {2}",
                   new Object[]{plan, md, actionDescription});

        final Admin admin = plan.getAdmin();

        /*
         * The threshold is a percent of groups. The resulting
         * number of groups must be > 0.
         */
        final int thresholdGroups =
              Math.max((nGroups * params.getBroadcastMetadataThreshold()) / 100,
                        1);
        final int acceptableFailures = nGroups - thresholdGroups;
        final long delay = params.getBroadcastMetadataRetryDelayMillis();

        int attempts = 0;

        /* Continue to retry until the threshold is met, or interrupted */
        while (retryList.size() > acceptableFailures) {

            /*
             * Get a new registry each attempt in case the failures from a
             * previous pass were network related
             */
            final RegistryUtils registry =
                new RegistryUtils(topo, plan.getLoginManager(),
                                  plan.getLogger());

            final Iterator<RepGroup> itr = retryList.iterator();

            while (itr.hasNext()) {
                boolean success = false;
                final RepGroup rg = itr.next();

                for (RepNode rn : rg.getRepNodes()) {

                    if (plan.isInterruptRequested()) {
                        logger.log(Level.INFO,
                                   "{0} has been interrupted, stop attempts " +
                                   "to broadcast {1} metadata changes for {2}",
                                   new Object[] {plan, md.getType(),
                                                 actionDescription});
                        return false;
                    }

                    final int result = sendMetadataChangesToRN(logger,
                                                              rn, md,
                                                              actionDescription,
                                                              registry,
                                                              masterOnly,
                                                              plan);

                    /*
                     * Newer MD was found in the wild. If this is topology
                     * broadcast (masterOnly == false) just exit. Otherwise,
                     * mark shard done, get new MD, and continue.
                     */
                    if (result == REFRESH) {
                        /* Exit if broadcasting topology. */
                        if (!masterOnly) {
                            return true;
                        }
                        success = true;

                        try {
                            final Metadata<?> newMd =
                                    plan.getAdmin().getMetadata(md.getClass(),
                                                                md.getType());
                            /*
                             * Should not happen, but be safe and continue with
                             * the original MD if the new MD is not found.
                             */
                            if (newMd != null) {
                                md = newMd;
                            }
                        } catch (Exception ex) {
                            /* ignore and continue with the original md */
                        }
                        break;
                    }

                    if (result == SUCCEEDED) {
                        success = true;

                        /*
                         * If only broadcasting to the master we are done
                         * with this group.
                         */
                        if (masterOnly) {
                            break;
                        }
                    }

                    /* Attempt failed, or node was not master, continue. */
                }

                /*
                 * If at least one member of the group has the MD, we are done
                 * with the group.
                 */
                if (success) {
                    itr.remove();
                }
            }

            /* Completed a pass through the groups */
            attempts++;

            if (retryList.isEmpty()) {
                logger.log(Level.INFO,
                           "{0} successful broadcast to all groups of {1} " +
                           "metadata seq# {2}, for {3}, attempts={4}",
                           new Object[]{plan, md.getType(),
                                        md.getSequenceNumber(),
                                        actionDescription, attempts});
                return true;
            }

            logger.log(Level.INFO,
                       "{0} broadcast {1} metadata to {2} out of {3} groups, " +
                       "will  retry, acceptable failure threshold={4}, " +
                       "attempts={5}",
                       new Object[]{plan, md.getType(),
                                    retryList.size(), nGroups,
                                    acceptableFailures, attempts});
            final MetadataType mdType = md.getType();
            final Supplier<String> stopMsg = () -> String.format(
                "stop attempts to broadcast %s metadata change for %s",
                mdType, actionDescription);
            try {
                if (admin.awaitUntilClosing(delay)) {
                    logger.log(Level.INFO,
                               "admin is shutting down during {0}, {1}",
                               new Object[] {plan, stopMsg.get()});
                    return false;
                }
            } catch (InterruptedException ie) {
                logger.log(Level.INFO,
                           "{0} has been interrupted, {1}",
                           new Object[] {plan, stopMsg.get()});
                return false;
            }
        }
        logger.log(Level.INFO,
                   "{0} broadcast {1} metadata {2} for {3} successful to {4} " +
                   "out of {5} groups",
                   new Object[]{plan, md.getType(),
                                md.getSequenceNumber(), actionDescription,
                                nGroups - retryList.size(), nGroups});
        return true;
    }

    /* Return values from sendMetadataChangesToRN() */
    private static final int SUCCEEDED = 1;
    private static final int FAILED = 0;
    private static final int REFRESH = -1;

    /**
     * Sends the list of metadata changes or the entire metadata to the
     * specified repNode.
     *
     * @return SUCCEEDED if the update was successful, FAILED if failed or the
     * node is not a master, or STOP if a newer metadata was found
     */
    private static int sendMetadataChangesToRN(Logger logger,
                                               RepNode rn,
                                               Metadata<?> md,
                                               String actionDescription,
                                               RegistryUtils registry,
                                               boolean masterOnly,
                                               AbstractPlan plan) {
        final StorageNodeId snId = rn.getStorageNodeId();
        final RepNodeId rnId = rn.getResourceId();

        try {
            final RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);

            /*
             * If we are only sending to masters and this node is not a
             * master, skip and return FAILED
             */
            if (masterOnly &&
                !rnAdmin.ping().getReplicationState().isMaster()) {
                return FAILED;
            }
            int rnSeqNum = rnAdmin.getMetadataSeqNum(md.getType());

            /* If the RN is behind, attempt to update it */
            if (rnSeqNum < md.getSequenceNumber()) {
                final MetadataInfo info = md.getChangeInfo(rnSeqNum);

                /* If the info is empty, send the full metadata. */
                if (info.isEmpty()) {
                    logger.log(Level.FINE,
                               "{0} unable to send {1} changes to {2} at {3}," +
                               " sending full metadata",
                               new Object[]{plan, md.getType(),
                                            rnId, rnSeqNum});

                    rnAdmin.updateMetadata(md);
                    return SUCCEEDED;
                }
                rnSeqNum = rnAdmin.updateMetadata(info);
            }

            /* Update was successful or the RN was already up-to-date */
            if (rnSeqNum == md.getSequenceNumber()) {
                return SUCCEEDED;
            }

            /*
             * Finding newer metadata in the wild means some other task
             * has an updated metadata and has sent it out. In this case
             * refresh and continue.
             *
             * For reference, this use to exit the broadcast. However that
             * caused the plan to exit early, before the store was completely
             * updated, causing confusion at the client.
             */
            if (rnSeqNum > md.getSequenceNumber()) {
                logger.log(Level.FINE,
                           "{0} has a higher {1} metadata sequence number of " +
                           "{2} compared to this metadata of {3} while {4}",
                           new Object[]{rnId, md.getType(), rnSeqNum,
                                        md.getSequenceNumber(),
                                        actionDescription});
                return REFRESH;
            }

            /*
             * If here, rnSeqNum < md.getSequenceNumber() meaning the update
             * failed. This could be due to benign problems or it could be due
             * to metadata corruption on the RN. To cover that case attempt to
             * update with the full metadata. Return fail so that this update
             * will be retried, hopefully with a successful outcome.
             */
            logger.log(Level.INFO,
                       "{0} update of {1} metadata to {2} on {3} failed. " +
                       "Expected metadata seq num: {4} actual: {5} while {6}," +
                       " sending full metadata",
                        new Object[]{plan, md.getType(), rnId, snId,
                                     md.getSequenceNumber(), rnSeqNum,
                                     actionDescription});
            rnAdmin.updateMetadata(md);
        } catch (RepNodeAdminFaultException rnfe) {
            /*
             * RN had problems with this request; often the problem is that
             * it is not in RUNNING state
             */
            logger.log(Level.INFO,
                      "{0} unable to update {1} metadata for {2} on {3} for " +
                      "{4} during broadcast: {5}",
                       new Object[]{plan, md.getType(), rnId, snId,
                                    actionDescription, rnfe.getMessage()});
        } catch (NotBoundException notbound) {
            logger.log(Level.INFO,
                       "{0} {1} on {2} cannot be contacted for {3} metadata " +
                       "update to {4} during broadcast: {5}",
                       new Object[]{plan, rnId, snId, md.getType(),
                                    actionDescription, notbound});
        } catch (RemoteException e) {
            logger.log(Level.INFO,
                       "{0} could not update {1} metadata for {2} on {3} for " +
                       "{4}: {5}",
                       new Object[]{plan, md.getType(), rnId, snId,
                                    actionDescription, e});
        }

        /*
         * Any other RuntimeExceptions indicate an unexpected problem, and will
         * fall through and throw out of this method.
         */
        return FAILED;
    }

    /**
     * For all members of this shard other than the skipId, write the new RN/AN
     * params to the owning SNA, and tell the RN/AN to refresh its params. Try
     * to be resilient; try all RNs, even in the face of a RMI failure from one.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static void refreshParamsOnPeers(AbstractPlan plan,
                                            ResourceId skipId)
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        final RepGroupId rgId;
        if (skipId instanceof RepNodeId) {
            rgId = topo.get((RepNodeId)skipId).getRepGroupId();
        } else {
            rgId = topo.get((ArbNodeId)skipId).getRepGroupId();
        }
        final RegistryUtils registry =
            new RegistryUtils(topo, plan.getLoginManager(),
                              plan.getLogger());
        plan.getLogger().log(Level.INFO,
                             "{0} writing new RN params to members of " +
                             "shard {1}",
                              new Object[]{plan, rgId});

        RemoteException remoteExSeen = null;
        NotBoundException notBoundSeen = null;

        for (RepNode peer : topo.get(rgId).getRepNodes()) {
            final RepNodeId peerId = peer.getResourceId();
            if (peerId.equals(skipId)) {
                /* Skip the relocated RN, it is not yet deployed */
                continue;
            }

            final RepNodeParams peerRNP = admin.getRepNodeParams(peerId);

            /* Write a new config file on the SNA */
            try {
                final StorageNodeAgentAPI sna =
                    registry.getStorageNodeAgent(peer.getStorageNodeId());
                sna.newRepNodeParameters(peerRNP.getMap());

                /* Have the RN notice its new params */
                final RepNodeAdminAPI rnAdmin =
                        registry.getRepNodeAdmin(peerId);
                rnAdmin.newParameters();
            } catch (RemoteException e) {
                /* Save the exception, carry on to all the others */
                remoteExSeen = e;
                plan.getLogger().log(Level.INFO,
                                     "{0} couldn''t refresh params on {1} {2}",
                                     new Object[]{plan, peerId, e});
            } catch (NotBoundException e) {
                notBoundSeen = e;
                plan.getLogger().log(Level.INFO,
                                     "{0} couldn''t refresh params on {1} {2}",
                                     new Object[]{plan, peerId, e});
            }
        }

        for (ArbNode peer : topo.get(rgId).getArbNodes()) {
            final ArbNodeId peerId = peer.getResourceId();
            if (peerId.equals(skipId)) {
                /* Skip the relocated AN, it is not yet deployed */
                continue;
            }

            final ArbNodeParams peerANP = admin.getArbNodeParams(peerId);

            /* Write a new config file on the SNA */
            try {
                final StorageNodeAgentAPI sna =
                    registry.getStorageNodeAgent(peer.getStorageNodeId());
                sna.newArbNodeParameters(peerANP.getMap());

                /* Have the RN notice its new params */
                final ArbNodeAdminAPI anAdmin =
                        registry.getArbNodeAdmin(peerId);
                anAdmin.newParameters();
            } catch (RemoteException e) {
                /* Save the exception, carry on to all the others */
                remoteExSeen = e;
                plan.getLogger().log(Level.INFO,
                                     "{0} couldn''t refresh params on {1} {2}",
                                     new Object[]{plan, peerId, e});
            } catch (NotBoundException e) {
                notBoundSeen = e;
                plan.getLogger().log(Level.INFO,
                                    "{0} couldn''t refresh params on {1} {2}",
                                    new Object[]{plan, peerId, e});
            }
        }

        /* Now throw the exception we've seen */
        if (remoteExSeen != null) {
            throw remoteExSeen;
        }

        if (notBoundSeen != null) {
            throw notBoundSeen;
        }
    }

    /**
     * Calculate the heap, cache, and GC params, which are a function of
     * the number of RNs on this SN, and set the appropriate values in the
     * RepNodeParams.
     */
    public static void setRNPHeapCacheGC(ParameterMap policyMap,
                                         StorageNodeParams targetSNP,
                                         RepNodeParams targetRNP,
                                         Topology topo) {

        final StorageNodeId targetSN = targetSNP.getStorageNodeId();
        final RepNodeId targetRN = targetRNP.getRepNodeId();

        /* How many RNs will be hosted on this SN? */
        final Set<RepNodeId> rnsOnSN = topo.getHostedRepNodeIds(targetSN);
        int numRNsOnSN = rnsOnSN.size();
        if (!rnsOnSN.contains(targetRN)) {
            numRNsOnSN += 1;
        }
        final int numANsOnSN = topo.getHostedArbNodeIds(targetSN).size();

        final RNHeapAndCacheSize heapAndCache =
            targetSNP.calculateRNHeapAndCache(policyMap,
                                              numRNsOnSN,
                                              targetRNP.getRNCachePercent(),
                                              numANsOnSN);
        targetRNP.setRNHeapAndJECache(heapAndCache);
        int gcThreads = targetSNP.calcGCThreads();
        targetRNP.setParallelGCThreads(gcThreads);

        /* Set the concurrent threads the same as parallel threads */
        targetRNP.setConcurrentGCThreads(gcThreads);
    }

    /**
     * Return true if a majority of nodes in the given are running.
     */
    public static boolean verifyZoneHealth(DatacenterId zoneId,
                                           Topology topo,
                                           Logger logger) {

        final Set<ResourceId> targets = new HashSet<>();
        targets.addAll(topo.getRepNodeIds(zoneId));
        targets.addAll(topo.getArbNodeIds(zoneId));

        final int expectedHealthyNodes = targets.size() / 2 + 1;
        int healthyNodes = 0;
        for (ResourceId rid : targets) {
            if (ServiceUtils.ping(rid, topo, logger).equals(
                    ServiceStatus.RUNNING)) {
                healthyNodes++;
            }
        }

        if (healthyNodes < expectedHealthyNodes) {
            return false;
        }
        return true;
    }

    /**
     * Gets the status from each of the store's Admins. If an error occurs
     * obtaining the status of an Admin it's status value will be
     * ServiceStatus.UNREACHABLE.
     *
     * @return a map of Admin IDs and status
     */
    static Map<AdminId, ServiceStatus> getAdminStatus(AbstractPlan plan,
                                                      Parameters params,
                                                      Logger logger) {
        final Map<AdminId, ServiceStatus> ret = new HashMap<>();
        final AdminId self =
                plan.getAdmin().getParams().getAdminParams().getAdminId();
        for (final AdminId adminId : params.getAdminIds()) {
            if (adminId.equals(self)) {
                ret.put(adminId, ServiceStatus.RUNNING);
                continue;
            }
            final StorageNodeId snId = params.get(adminId).getStorageNodeId();
            final StorageNodeParams snp = params.get(snId);
            AdminStatus status = null;
            try {
                final CommandServiceAPI admin =
                        RegistryUtils.getAdmin(snp.getHostname(),
                                               snp.getRegistryPort(),
                                               plan.getLoginManager(),
                                               logger);
                status = admin.getAdminStatus();
            } catch (RemoteException | NotBoundException re) {
            }
            ret.put(adminId, status == null ? ServiceStatus.UNREACHABLE :
                                              status.getServiceStatus());
        }
        return ret;
    }

    /**
     * Returns true if and only if all nodes in the store have the target
     * version or later.
     *
     * @param admin admin as an entry of the store
     * @param targetVersion the version to be checked
     * @return true iff. the all nodes in the store have the target version or
     * later.
     * @throws IllegalCommandException if cannot obtain the store version
     */
    public static boolean storeHasVersion(Admin admin,
                                          KVVersion targetVersion) {
        final KVVersion storeVersion;
        try {
            storeVersion = admin.getStoreVersion();
        } catch (AdminFaultException e) {
            throw new IllegalCommandException(
                String.format(
                    "Unable to confirm that all nodes in the store have the " +
                    "required version of %s or later",
                    targetVersion.getNumericVersionString()),
                e);
        }
        return VersionUtil.
            compareMinorVersion(storeVersion, targetVersion) >= 0;
    }

    /**
     * Gets the AdminType based on the Datacenter type
     */
    public static AdminType getAdminType(DatacenterType dcType) {
        switch (dcType) {
        case PRIMARY : return AdminType.PRIMARY;
        case SECONDARY : return AdminType.SECONDARY;
        }
        throw new IllegalStateException("Unknown datacenter type: " + dcType);
    }

    /** Ensure that the first created user is enabled and an Admin. */
    static void ensureFirstAdminUser(SecurityMetadata secMd,
                                     boolean isEnabled,
                                     boolean isAdmin) {
        if ((secMd == null) || secMd.getAllUsers().isEmpty()) {
            if (!(isAdmin && isEnabled)) {
                throw new IllegalCommandException(
                    "The first user in the store must be -admin and enabled.");
            }
        }
    }

    /**
     * Checks whether a pre-existing user has same enabled, admin and password
     * attributes as the requested new one.
     *
     * @throws IllegalCommandException  If any of the enabled state, the admin
     * role and the password between the pre-existing user and the new
     * requested user is different.
     */
    static void checkPreExistingUser(SecurityMetadata secMd,
                                     String userName,
                                     boolean isEnabled,
                                     boolean isAdmin,
                                     char[] plainPassword) {
        if (secMd == null) {
            return;
        }
        final KVStoreUser preExistUser = secMd.getUser(userName);

        if (preExistUser != null) {
            if (preExistUser.isEnabled() != isEnabled) {
                throw new IllegalCommandException(
                    "User with name " + preExistUser.getName() +
                    " already exists but has enabled state of " +
                    preExistUser.isEnabled() +
                    " rather than the requested enabled state of " + isEnabled);
            }
            if (preExistUser.isAdmin() != isAdmin) {
                throw new IllegalCommandException(
                    "User with name " + preExistUser.getName() +
                    " already exists but has admin setting of " +
                    preExistUser.isAdmin() + " rather than the requested " +
                    "admin setting of " + isAdmin);
            }
            if (plainPassword != null &&
                !preExistUser.verifyPassword(plainPassword)) {
                throw new IllegalCommandException(
                    "User with name " + userName +
                    " already exists but has different password than the" +
                    " requested one.");
            }
        }
    }

    /**
     * Acquire Kerberos information from storage node and store in security
     * metadata. Before storing Kerberos information, check if realm and service
     * name of each storage node principal are the same, otherwise throw
     * IllegalArgumentException.
     *
     * @return true if storing new Kerberos information in security metadata.
     */
    public static boolean storeKerberosInfo(AbstractPlan plan,
                                            SecurityMetadata md,
                                            Logger logger)
        throws RemoteException, NotBoundException, IllegalArgumentException {

        final Admin admin = plan.getAdmin();
        final Parameters parameters = admin.getCurrentParameters();
        if (md == null) {
            final String storeName = parameters.getGlobalParams()
                                               .getKVStoreName();
            md = new SecurityMetadata(storeName);
        }
        final Topology topo = admin.getCurrentTopology();
        final RegistryUtils regUtils = new RegistryUtils(
            topo, admin.getLoginManager(), logger);

        String realmName = null;
        String serviceName = null;
        boolean storeMd = false;

        for (StorageNodeId snId : topo.getSortedStorageNodeIds()) {
            final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
            final KrbPrincipalInfo prinInfo = sna.getKrbPrincipalInfo();

            /* Check realm and service name are configured correctly */
            final String realm = prinInfo.getRealmName();
            if (realmName == null) {
                realmName = realm;
            } else if (!realm.equals(realmName)) {
                throw new IllegalArgumentException("Principals of all nodes " +
                    "in the same store are expected to be in the same realm");
            }

            final String service = prinInfo.getServiceName();
            if (serviceName == null) {
                serviceName = service;
            } else if (!serviceName.equals(service)) {
                throw new IllegalArgumentException("Principals of all nodes " +
                    "in the same store must have the same service name");
            }

            final String instance = prinInfo.getInstanceName();
            if (instance != null && !instance.equals("") &&
                md.addKerberosInstanceName(instance, snId) != null) {
                storeMd = true;
            }
        }

        if (storeMd) {
            admin.saveMetadata(md, plan);
            return true;
        }
        return false;
    }

    /**
     * Deletes the old RN on the original SN. Returns SUCCESS if the delete was
     * successful. This method calls awaitConsistency() on the new node
     * to make sure it is up and healthy before deleting the old node.
     *
     * @return SUCCESS if the old RN was deleted
     * @throws InterruptedException
     */
    public static boolean destroyRepNode(AbstractPlan plan,
                                         long stopRNTime,
                                         StorageNodeId targetSNId,
                                         RepNodeId targetRNId)
        throws InterruptedException {
        return waitDestroyRepNodeInternal(plan,
                                          stopRNTime,
                                          targetSNId,
                                          targetRNId, true /* destroyTarget*/);

    }

    public static boolean waitForRepNode(AbstractPlan plan,
                                         RepNodeId targetRNId)
        throws InterruptedException {
        return waitDestroyRepNodeInternal(plan,
                                          System.currentTimeMillis(),
                                          null /* SNId */,
                                          targetRNId,
                                          false /* destroyTarget */);
    }

    /**
     * Deletes the old AN on the original SN. Returns SUCCESS if the delete was
     * successful.
     *
     * @return SUCCESS if the old AN was deleted
     */
    public static boolean destroyArbNode(Admin admin,
                                         AbstractPlan plan,
                                         StorageNodeId targetSNId,
                                         ArbNodeId targetANId) {

        final Topology useTopo = admin.getCurrentTopology();
        final RegistryUtils registry =
            new RegistryUtils(useTopo, admin.getLoginManager(),
                              plan.getLogger());

        try {
            plan.getLogger().log(Level.INFO,
                       "{0} attempting to delete {0} from {1}",
                       new Object[]{plan, targetANId, targetSNId});

            final StorageNodeAgentAPI oldSna =
                registry.getStorageNodeAgent(targetSNId);
            oldSna.destroyArbNode(targetANId, true /* deleteData */,
                                  plan.toString());
            return true;
        } catch (RemoteException re) {
            plan.getLogger().log(Level.INFO,
                                 "{0} remote call to {1} failed with {2}",
                                 new Object[]{plan, targetANId,
                                              re.getLocalizedMessage()});

        } catch (NotBoundException nbe) {
            plan.getLogger().log(Level.INFO,
                                 "{0} registry call failed with {1}",
                                 new Object[]{plan, nbe.getLocalizedMessage()});

        }
        return false;
    }

    /**
     * Waits for RN to be consistent with passed in time and optionally
     * destroy the RN.
     *
     * @param plan the plan
     * @param stopRNTime time used for awaitConsistency
     * @param targetSNId SN used only if destroying target
     * @param targetRNId RN to wait for
     * @param destroyTarget if true RN is destroyed
     *
     * @return true if successful, false if
     *         RN was not consistent with stopRNTime
     * @throws InterruptedException
     */
    private static boolean waitDestroyRepNodeInternal(AbstractPlan plan,
                                                      long stopRNTime,
                                                      StorageNodeId targetSNId,
                                                      RepNodeId targetRNId,
                                                      boolean destroyTarget)
        throws InterruptedException {

        /* Delay between calls to the awaitConsistency after an error */
        final int WAIT_FOR_CONSISTENCY_DELAY_MS = 1000 * 60;

        /* Wait a minute between consistency checks */
        final int WAIT_FOR_CONSISTENCY = 60;

        final Admin admin = plan.getAdmin();

        final long endCheckAtThisTime = System.currentTimeMillis() +
            admin.getParams().getAdminParams().getAwaitRNConsistencyPeriod();

        final Topology useTopo = admin.getCurrentTopology();
        RegistryUtils registry =
            new RegistryUtils(useTopo, admin.getLoginManager(),
                              plan.getLogger());
        do {
            if (destroyTarget) {
                plan.getLogger().log(Level.INFO,
                                     "{0} waiting for {1} to become " +
                                     "consistent before removing it from {2}." +
                                     " Topology says it is on {3}",
                                     new Object[]{plan, targetRNId, targetSNId,
                                   useTopo.get(targetRNId).getStorageNodeId()});
            } else {
                plan.getLogger().log(Level.INFO,
                                     "{0} waiting for {1} to become consistent",
                                     new Object[]{plan, targetRNId});
            }

            try {
                final RepNodeAdminAPI rnAdmin =
                    registry.getRepNodeAdmin(targetRNId);

                if (rnAdmin.awaitConsistency(stopRNTime, WAIT_FOR_CONSISTENCY,
                                             TimeUnit.SECONDS)) {
                    if (destroyTarget) {
                        plan.getLogger().log(Level.INFO,
                                             "{0} attempting to delete {1} " +
                                             "from {2}",
                                             new Object[]{plan,
                                                          targetRNId,
                                                          targetSNId});

                        final StorageNodeAgentAPI oldSna =
                            registry.getStorageNodeAgent(targetSNId);
                        oldSna.destroyRepNode(targetRNId,
                                              true /* deleteData */,
                                              plan.toString());
                    }
                    return true;
                }
            } catch (RemoteException re) {

                /*
                 * Since we have gotten this far, we should do our best to
                 * finish. The call to awaitConsistency may fail due to various
                 * network issues or the RN not yet started or is very busy
                 * starting up. This last case can happen if
                 * WAIT_FOR_CONSISTENCY_MS > the socket timeout.
                 */
                plan.getLogger().log(Level.INFO,
                                     "{0} remote call to {1} failed with {2}",
                                     new Object[]{plan, targetRNId,
                                                  re.getLocalizedMessage()});

                /*
                 * If we have not timed-out, sleep for a short bit to avoid
                 * spinning on a network error.
                 */
                if (endCheckAtThisTime > System.currentTimeMillis()) {
                    Thread.sleep(WAIT_FOR_CONSISTENCY_DELAY_MS);
                }
            } catch (NotBoundException nbe) {
                plan.getLogger().log(Level.INFO,
                                     "{0} registry call failed with {2}",
                                     new Object[]{plan,
                                                  nbe.getLocalizedMessage()});

                if (endCheckAtThisTime > System.currentTimeMillis()) {
                    Thread.sleep(WAIT_FOR_CONSISTENCY_DELAY_MS);
                }

                /* Reacquire the registry */
                registry = new RegistryUtils(admin.getCurrentTopology(),
                                             admin.getLoginManager(),
                                             plan.getLogger());
            }
        } while (endCheckAtThisTime > System.currentTimeMillis());
        return false;
    }

    public static RepGroupId getRepGroupId(ResourceId resId) {
        if (resId instanceof RepNodeId) {
            return new RepGroupId(((RepNodeId)resId).getGroupId());
        } else  if (resId instanceof ArbNodeId) {
            return new RepGroupId(((ArbNodeId)resId).getGroupId());
        } else {
            return null;
        }
    }

    /**
     * Ensure that all existing SNs are in harmony regarding the presence or
     * lack thereof of a search cluster.  If they are, then return the search
     * cluster's connection information in a ParameterMap.
     */
    public static ParameterMap verifyAndGetSearchParams(Parameters p) {

        final String remedy =
            "\nTo correct this problem, please issue the register-es command" +
            " to re-register the search cluster. ";

        String searchClusterName = "";
        String searchClusterMembers = "";
        boolean searchClusterSecure = true; // Default value for secure ES
                                            // Cluster.
        int counter = 0;
        for (StorageNodeParams sp : p.getStorageNodeParams()) {
            final String name = sp.getSearchClusterName();
            final String members = sp.getSearchClusterMembers();
            final boolean isSecure = sp.isSearchClusterSecure();

            /* First verify that the two parameters are either both null or
             * both non-null -- either both should be set or neither.
             * This really should not be possible, but check it anyway.
             */
            if (("".equals(name)) != ("".equals(members))) {
                throw new CommandFaultException
                    (sp.getStorageNodeId() + "'s search cluster parameters " +
                     "are not consistent." + remedy,
                     ErrorMessage.NOSQL_5200,
                     CommandResult.NO_CLEANUP_JOBS);
            }

            /* Now check for inconsistency between SNs */
            if (counter > 0 &&
                (!(searchClusterName.equals(name) &&
                            searchClusterMembers.equals(members) &&
                            (searchClusterSecure == isSecure)))) {
                throw new CommandFaultException
                    (sp.getStorageNodeId() + "'s search cluster parameters " +
                     "do not match other SNs." + remedy,
                     ErrorMessage.NOSQL_5200,
                     CommandResult.NO_CLEANUP_JOBS);
            }

            searchClusterName = name;
            searchClusterMembers = members;
            searchClusterSecure = isSecure;
            counter++;
        }

        final ParameterMap pm = new ParameterMap(ParameterState.SNA_TYPE,
                                                 ParameterState.SNA_TYPE);
        pm.setParameter
            (ParameterState.SN_SEARCH_CLUSTER_NAME, searchClusterName);
        pm.setParameter
            (ParameterState.SN_SEARCH_CLUSTER_MEMBERS, searchClusterMembers);
        pm.setParameter(ParameterState.SN_SEARCH_CLUSTER_SECURE,
                        Boolean.toString(searchClusterSecure));
        return pm;
    }

    /**
     * Select the storage directory from target SN for the RN to move to. If
     * the size of RN is larger than all of SN's storage directories, null is
     * returned. If the SN's storage directories are all filled in with other
     * RN, null is returned. If a candidate storage directory is selected,
     * the selected storage directory is returned.
     * @param snp the storage node parameter of the target SN.
     * @param size the mount point size of the RN to be moved.
     * @param assignedRns a map recording which RN has been assigned to SN's
     * directory already.
     */
    public static StorageDirectory
        findStorageDir(StorageNodeParams snp,
                       long size,
                       Map<RepNodeId, StorageDirectory> assignedRns) {
        /*
         * If there are defined storage directories, try to find the smallest
         * one that works.
         */
        StorageDirectory candidate = null;
        final ParameterMap storageDirMap = snp.getStorageDirMap();
        if (storageDirMap != null) {
            for (Parameter p : storageDirMap) {
                final StorageDirectory proposed = new StorageDirectory(p);
                if ((proposed.getSize() >= size) &&
                    !assignedRns.containsValue(proposed)) {
                    /*
                     * We have a proposed directory that works, check if it is
                     * smaller than the current candidate if there is one.
                     */
                    if ((candidate == null) ||
                        (proposed.getSize() < candidate.getSize())) {
                        candidate = proposed;
                    }
                }
            }
        }

        if (candidate != null) {
            return candidate;
        }

        /*
         * No explicit storage directories found, attempt to put in root
         * dir if possible
         */
        final int numExplicitDirs = (storageDirMap == null) ? 0 :
                                                       storageDirMap.size();
        if ((snp.getCapacity() > numExplicitDirs) &&
            (snp.getRootDirSize() >= size)) {
            return new StorageDirectory(null, snp.getRootDirSize());
        }
        return null;
    }

    static void checkCreateUserPwPolicies(char[] password,
                                          Admin admin,
                                          String userName) {
        final ParameterMap map =
            admin.getCurrentParameters().getPolicies();
        final String storeName =
            admin.getGlobalParams().getKVStoreName();
        if (map.getOrDefault(ParameterState.SEC_PASSWORD_COMPLEXITY_CHECK).
                asBoolean()) {
            final PasswordChecker checker =
                PasswordCheckerFactory.createCreateUserPassChecker(
                    map, userName, storeName);
            makePasswordCheck(checker, password);
        }
    }

    static void checkAlterUserPwPolicies(char[] password,
                                         Admin admin,
                                         KVStoreUser userMd) {
        final ParameterMap map =
            admin.getCurrentParameters().getPolicies();
        final String storeName =
            admin.getGlobalParams().getKVStoreName();
        if (password != null &&
            map.getOrDefault(ParameterState.SEC_PASSWORD_COMPLEXITY_CHECK).
                asBoolean()) {
            final PasswordChecker checker =
                PasswordCheckerFactory.createAlterUserPassChecker(
                    map, storeName, userMd);
            makePasswordCheck(checker, password);
        }
    }

    static private void makePasswordCheck(PasswordChecker checker,
                                          char[] password) {
        final char[] copiedPass =
            Arrays.copyOf(password, password.length);
        try {
            final PasswordCheckerResult result =
                checker.checkPassword(copiedPass);
            if (!result.isPassed()) {
                throw new IllegalCommandException(result.getMessage());
            }
        } finally {
            SecurityUtils.clearPassword(copiedPass);
        }
    }

    /**
     * Finds the master of the specified shard. Returns the admin interface
     * for the master RN or null if it can't be found. Throws
     * ShardNotFoundException if the shard is not in the topology.
     *
     * @return null if no master can be found, otherwise the admin interface
     * for the master RN.
     *
     * @throws ShardNotFoundException if the shard is no longer in the topology
     * @throws NotBoundException
     * @throws RemoteException
     */
    static RepNodeAdminAPI getMaster(AbstractPlan ap, RepGroupId rgId)
        throws RemoteException, NotBoundException, ShardNotFoundException {

        final Admin admin = ap.getAdmin();
        final Topology topology = admin.getCurrentTopology();

        if (topology.get(rgId) == null) {
            /* Shard is gone due to contraction */
            throw new ShardNotFoundException(rgId, topology);
        }
        final PingCollector collector =
            new PingCollector(topology, ap.getLogger());
        final RepNode masterRN = collector.getMaster(rgId);
        if (masterRN == null) {
            return null;
        }

        final RegistryUtils registryUtils =
                new RegistryUtils(topology, admin.getLoginManager(),
                                  ap.getLogger());
        return registryUtils.getRepNodeAdmin(masterRN.getResourceId());
    }

    static class ShardNotFoundException extends Exception {
        private static final long serialVersionUID = 1L;

        ShardNotFoundException(RepGroupId rgId, Topology topology) {
            super("Shard " + rgId.getGroupName() +
                  " is not in topology seq# " + topology.getSequenceNumber());
        }
    }
}
