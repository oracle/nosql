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

package oracle.kv.impl.admin;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;

/**
 * Utility class for upgrade operations.
 */
public class UpgradeUtil {

    /* Prevent construction */
    private UpgradeUtil() {}

    /**
     * Generate the list of SNs that need to be upgraded. The list will
     * contain only the nodes which have a software version less than
     * targetVersion and meet the prerequisiteVersion. If a node requires
     * an upgrade but does not meet the prerequisite an error message is
     * returned.
     *
     * Each line of the generated list will include at least one SN. If other
     * SNs can be safely upgraded at the same time, those will be included on
     * the line separated by a space.
     *
     * The rules for determining which SNs can be upgraded together are:
     * - The SNs cannot host an RN from the same shard
     * - Only on Admin can be upgraded at one time
     *
     * @param admin
     * @param targetVersion
     * @param prerequisiteVersion
     * @return a list of SNs
     */
    static String generateUpgradeList(Admin admin,
                                      KVVersion targetVersion,
                                      KVVersion prerequisiteVersion) {

        final StringBuilder sb = new StringBuilder();
        sb.append("Calculating upgrade order, target version: ");
        sb.append(targetVersion.getNumericVersionString());
        sb.append(", prerequisite: ");
        sb.append(prerequisiteVersion.getNumericVersionString());

        final Logger logger = admin.getLogger();
        logger.info(sb.toString());

        sb.append("\n");

        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(), logger);
        final Map<StorageNodeId, AdminId> needsUpgrade =
                                    new HashMap<StorageNodeId, AdminId>();

        /* Find the SNs which need to be upgraded */

        for (StorageNodeId snId : topology.getStorageNodeIds()) {
            StorageNodeStatus snStatus = null;

            logger.log(Level.FINE, "Checking {0}", snId);
            try {
                final StorageNodeAgentAPI sna =
                            registryUtils.getStorageNodeAgent(snId);
                snStatus = sna.ping();
            } catch (RemoteException re) {
                sb.append("Unable to contact ");
                sb.append(snId.toString());
                sb.append(" ");
                sb.append(re.getMessage());
                sb.append("\n");
                continue;
            } catch (NotBoundException nbe) {
                sb.append("Unable to contact ");
                sb.append(snId.toString());
                sb.append(" ");
                sb.append(nbe.getMessage());
                sb.append("\n");
                continue;
            }

            final KVVersion snVersion = snStatus.getKVVersion();

            /* Check for same or newer version */
            if (snVersion.compareTo(targetVersion) >= 0) {

                /* If too new, report a problem */
                if (VersionUtil.compareMinorVersion(snVersion,
                                                    targetVersion) > 0) {
                    sb.append("Cannot upgrade ");
                    sb.append(snId.toString());
                    sb.append(" which is at a newer minor version ");
                    sb.append(snVersion.getNumericVersionString());
                    sb.append("\n");
                }
                continue;
            }

            /* SN at older version. Check for meeting prereq. */
            if (snVersion.compareTo(prerequisiteVersion) < 0) {
                sb.append("Cannot upgrade ");
                sb.append(snId.toString());
                sb.append(" at version ");
                sb.append(snVersion.getNumericVersionString());
                sb.append(" which does not meet the prerequisite");
                sb.append("\n");
                continue;
            }

            /*
             * Storage node is good to go, check its RNs/ANs to make sure that
             * they are at the same version as the SN.
             */
            final String result = checkRNANs(snVersion,
                                             topology.getHostedRepNodeIds(snId),
                                             topology.getHostedArbNodeIds(snId),
                                             registryUtils);
            /* If there is a problem, report it and skip it */
            if (result != null) {
                sb.append(snId);
                sb.append(" needs upgrading, but there is an issue\n");
                sb.append(result);
                sb.append("\n");
                continue;
            }
            needsUpgrade.put(snId, null);
        }

        logger.log(Level.FINE, "{0} SNs need upgrading", needsUpgrade.size());

        if (needsUpgrade.isEmpty()) {
            sb.append("There are no nodes that need to be upgraded");
            return sb.toString();
        }

        /* Tag SNs that have admins */
        final Parameters p = admin.getCurrentParameters();

        for (AdminParams ap : p.getAdminParams()) {
            /* getStorageNodeId may return 0 */
            if (needsUpgrade.containsKey(ap.getStorageNodeId())) {
                needsUpgrade.put(ap.getStorageNodeId(), ap.getAdminId());
            }
        }

        while (!needsUpgrade.isEmpty()) {
            final Iterator<Entry<StorageNodeId, AdminId>> itr =
                                        needsUpgrade.entrySet().iterator();

            /* Pick an SN to upgrade */
            final Entry<StorageNodeId, AdminId> entry = itr.next();
            final StorageNodeId snId = entry.getKey();
            final boolean hasAdmin = (entry.getValue() != null);
            sb.append(snId.toString());
            itr.remove();

            final Set<Integer> affectedShards = new HashSet<Integer>();

            for (RepNodeId rnId : topology.getHostedRepNodeIds(snId)) {
                affectedShards.add(rnId.getGroupId());
            }

            logger.log(Level.FINE, "Next upgrade is {0} has admin: {1}",
                       new Object[]{snId, entry.getValue()});

            /* Check if any other SN can be upgraded at the same time */
            while (itr.hasNext()) {
                final Entry<StorageNodeId, AdminId> candidate = itr.next();
                final StorageNodeId candidateId = candidate.getKey();

                logger.log(Level.FINE, "Candidate is {0} has admin: {1}",
                           new Object[]{candidateId, candidate.getValue()});

                /* Check for them both having admins */
                if (hasAdmin && (candidate.getValue() != null)) {
                    continue;
                }

                final Set<RepNodeId> hostedRNs =
                                topology.getHostedRepNodeIds(candidateId);

                boolean OK = true;

                /* Check if a shard on this SN is already being upgraded */
                for (RepNodeId rnId : hostedRNs) {
                    if (affectedShards.contains(rnId.getGroupId())) {
                        OK = false;
                        break;
                    }
                }

                final Set<ArbNodeId> hostedANs =
                                topology.getHostedArbNodeIds(candidateId);
                if (OK) {
                    for (ArbNodeId anId : hostedANs) {
                        if (affectedShards.contains(anId.getGroupId())) {
                            OK = false;
                            break;
                        }
                    }

                }

                if (OK) {
                    logger.log(Level.FINE, "Adding {0}", candidateId);

                    itr.remove();
                    sb.append(" ");
                    sb.append(candidateId.toString());

                    /* Add the new shards which would be affected */
                    for (RepNodeId rnId : hostedRNs) {
                        affectedShards.add(rnId.getGroupId());
                    }
                    for (ArbNodeId anId : hostedANs) {
                        affectedShards.add(anId.getGroupId());
                    } 
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Generate the list of SNs that need to be upgraded. The list will
     * contain only the nodes which have a software version less than
     * targetVersion and meet the prerequisiteVersion. If a node requires
     * an upgrade but does not meet the prerequisite an error message is
     * returned.
     *
     * The rules for determining which SNs can be upgraded together are:
     * - The SNs cannot host an RN from the same shard
     * - Only on Admin can be upgraded at one time
     *
     * @param admin
     * @param targetVersion
     * @param prerequisiteVersion
     * @return a list of SN set, all SNs in the same set can be upgraded in
     * parallel.
     */
    static List<Set<StorageNodeId>>
        generateUpgradeOrderList(Admin admin,
                                 KVVersion targetVersion,
                                 KVVersion prerequisiteVersion) {
        final Logger logger = admin.getLogger();
        logger.info("Calculating upgrade order, target version: " +
                    targetVersion.getNumericVersionString() +
                    ", prerequisite: " +
                    prerequisiteVersion.getNumericVersionString());

        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(), logger);
        final Map<StorageNodeId, AdminId> needsUpgrade =
            new HashMap<StorageNodeId, AdminId>();

        for (StorageNodeId snId : topology.getStorageNodeIds()) {
            StorageNodeStatus snStatus = null;

            try {
                final StorageNodeAgentAPI sna =
                            registryUtils.getStorageNodeAgent(snId);
                snStatus = sna.ping();
            } catch (RemoteException re) {
                throw new IllegalCommandException(
                    "Fail to ping " + snId, re, ErrorMessage.NOSQL_5300,
                    new String[] {});
            } catch (NotBoundException nbe) {
                throw new IllegalCommandException(
                    "Fail to ping " + snId, nbe, ErrorMessage.NOSQL_5300,
                    new String[] {});
            }

            final KVVersion snVersion = snStatus.getKVVersion();

            /* Check for same or newer version */
            if (snVersion.compareTo(targetVersion) >= 0) {
                continue;
            }

            /* SN at older version. Check for meeting prereq. */
            if (snVersion.compareTo(prerequisiteVersion) < 0) {
                throw new IllegalCommandException(
                    "Cannot upgrade " + snId.toString() +
                    " at version " +
                    snVersion.getNumericVersionString() +
                    "which does not meet the prerequisite\n",
                    ErrorMessage.NOSQL_5200, new String[] {});
            }

            /*
             * Storage node is good to go, check its RN/ANs to make sure that
             * they are at the same version as the SN.
             */
            final String result = checkRNANs(snVersion,
                                             topology.getHostedRepNodeIds(snId),
                                             topology.getHostedArbNodeIds(snId),
                                             registryUtils);
            /* If there is a problem, report it and skip it */
            if (result != null) {
                throw new IllegalCommandException(
                    snId +
                    " needs upgrading, but there is an issue. " +
                    result,
                    ErrorMessage.NOSQL_5200, new String[] {});
            }
            needsUpgrade.put(snId, null);
        }

        final List<Set<StorageNodeId>> snList =
            new ArrayList<Set<StorageNodeId>>();
        if (needsUpgrade.isEmpty()) {
            return snList;
        }

        /* Tag SNs that have admins */
        final Parameters p = admin.getCurrentParameters();

        for (AdminParams ap : p.getAdminParams()) {
            /* getStorageNodeId may return 0 */
            if (needsUpgrade.containsKey(ap.getStorageNodeId())) {
                needsUpgrade.put(ap.getStorageNodeId(), ap.getAdminId());
            }
        }

        while (!needsUpgrade.isEmpty()) {
            final Set<StorageNodeId> snSet = new HashSet<StorageNodeId>();
            final Iterator<Entry<StorageNodeId, AdminId>> itr =
                needsUpgrade.entrySet().iterator();

            /* Pick an SN to upgrade */
            final Entry<StorageNodeId, AdminId> entry = itr.next();
            final StorageNodeId snId = entry.getKey();
            final boolean hasAdmin = (entry.getValue() != null);
            snSet.add(snId);
            itr.remove();

            final Set<Integer> affectedShards = new HashSet<Integer>();

            for (RepNodeId rnId : topology.getHostedRepNodeIds(snId)) {
                affectedShards.add(rnId.getGroupId());
            }
            
            for (ArbNodeId anId : topology.getHostedArbNodeIds(snId)) {
                affectedShards.add(anId.getGroupId());
            }

            /* Check if any other SN can be upgraded at the same time */
            while (itr.hasNext()) {
                final Entry<StorageNodeId, AdminId> candidate = itr.next();
                final StorageNodeId candidateId = candidate.getKey();

                /* Check for them both having admins */
                if (hasAdmin && (candidate.getValue() != null)) {
                    continue;
                }

                final Set<RepNodeId> hostedRNs =
                                topology.getHostedRepNodeIds(candidateId);
                
                final Set<ArbNodeId> hostedANs =
                                topology.getHostedArbNodeIds(candidateId);

                boolean OK = true;

                /* Check if a shard on this SN is already being upgraded */
                for (RepNodeId rnId : hostedRNs) {
                    if (affectedShards.contains(rnId.getGroupId())) {
                        OK = false;
                        break;
                    }
                }
                
                if (OK) {
                    for (ArbNodeId anId : hostedANs) {
                        if (affectedShards.contains(anId.getGroupId())) {
                            OK = false;
                            break;
                        }
                    } 
                }

                if (OK) {
                    itr.remove();
                    snSet.add(candidateId);

                    /* Add the new shards which would be affected */
                    for (RepNodeId rnId : hostedRNs) {
                        affectedShards.add(rnId.getGroupId());
                    }
                    for (ArbNodeId anId : hostedANs) {
                        affectedShards.add(anId.getGroupId());
                    }
                }
            }
            snList.add(snSet);
        }
        return snList;
    }

    /**
     * Checks if the specified RNs and ANs are at the storage node version.
     * Returns a list of nodes which differ from the SN version. If all nodes
     * are up-to-date null is returned.
     *
     * @param snVersion the storage node version
     * @param hostedRepNodeIds list of replication nodes to check
     * @param hostedArbNodeIds list of arbiter nodes to check
     * @param registryUtils
     * @return list of nodes not matching the SN version or null
     */
    private static String checkRNANs(KVVersion snVersion,
                                     Set<RepNodeId> hostedRepNodeIds,
                                     Set<ArbNodeId> hostedArbNodeIds,
                                     RegistryUtils registryUtils) {

        for (RepNodeId rnId : hostedRepNodeIds) {
            try {
                final RepNodeAdminAPI rn = registryUtils.getRepNodeAdmin(rnId);
                final KVVersion rnVersion = rn.getInfo().getSoftwareVersion();

                if (rnVersion.compareTo(snVersion) != 0) {
                    return "RepNode " + rn.toString() + " at " +
                           rnVersion.getNumericVersionString() +
                           " is not at the same software version as " +
                           "its hosting Storage Node";
                }
            } catch (RemoteException re) {
                return "Unable to contact " + rnId.toString() + " " +
                       re.getMessage();
            } catch (NotBoundException nbe) {
                return "Unable to contact " + rnId.toString() + " " +
                       nbe.getMessage();
            }
        }

        for (ArbNodeId anId : hostedArbNodeIds) {
            try {
                final ArbNodeAdminAPI an = registryUtils.getArbNodeAdmin(anId);
                final KVVersion anVersion = an.getInfo().getSoftwareVersion();

                if (anVersion.compareTo(snVersion) != 0) {
                    return "ArbNode " + an.toString() + " at " +
                           anVersion.getNumericVersionString() +
                           " is not at the same software version as " +
                           "its hosting Storage Node";
                }
            } catch (RemoteException re) {
                return "Unable to contact " + anId.toString() + " " +
                       re.getMessage();
            } catch (NotBoundException nbe) {
                return "Unable to contact " + anId.toString() + " " +
                       nbe.getMessage();
            }
        }
        return null;
    }
}
