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

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortRange;

import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

/**
 * The class holds a variety of utility methods used by VerifyConfiguration,
 * TopologyCheck, and plan tasks that manipulate topologies.
 */
public class TopologyCheckUtils {

    /**
     * Peruse both the topology and AdminDB params and return all
     * StorageNodeIds, RepNodeIds, ArbNodeIds, and AdminIds,
     * clustered and ordered by SN.
     */
    static Map<StorageNodeId, SNServices>
        groupServicesBySN(Topology topology, Parameters params) {

        Map<StorageNodeId, SNServices> resourceInfo =
            new TreeMap<StorageNodeId, SNServices>(new SNNameComparator());

        /*
         * Put an entry for every SN in the topology, whether or not the topo
         * thinks it owns any RNs or Admins. That ensures we'll check SNs
         * that have inconsistencies between the topo and the config.xml such
         * that the topo thinks they are uninhabited, but the config.xml thinks
         * it has services.
         */
        List<StorageNode> sortedSNs = topology.getSortedStorageNodes();
        for (StorageNode sn: sortedSNs) {
            resourceInfo.put(sn.getResourceId(),
                             new SNServices(sn.getResourceId()));
        }

        for (RepGroup rg: topology.getRepGroupMap().getAll()) {
            for (RepNode rn: rg.getRepNodes()) {
                StorageNodeId target = rn.getStorageNodeId();
                SNServices snInfo = resourceInfo.get(target);
                snInfo.add(rn.getResourceId());
            }
            for (ArbNode arb : rg.getArbNodes()) {
                StorageNodeId target = arb.getStorageNodeId();
                SNServices snInfo = resourceInfo.get(target);
                snInfo.add(arb.getResourceId());
            }
        }

        for (AdminId adId : params.getAdminIds()) {
            StorageNodeId target = params.get(adId).getStorageNodeId();
            SNServices snInfo = resourceInfo.get(target);
            if (snInfo == null) {
                snInfo = new SNServices(target);
                resourceInfo.put(target, snInfo);
            }
            snInfo.add(adId);
        }

        return resourceInfo;
    }

    /**
     * Sort by SN id, for displaying information from the verify command.
     */
    static class SNNameComparator implements Comparator<StorageNodeId> {
        @Override
        public int compare(StorageNodeId o1, StorageNodeId o2) {
            return o1.getStorageNodeId() - o2.getStorageNodeId();
        }
    }

    /**
     * Get JE HA rep group metadata using the JE interfaces. The information
     * is only available if there is a group master.
     *
     * @return the composition of the group as understood by JE HA. Set will
     * be empty if the information can't be retrieved.
     */
    static Set<ReplicationNode> getJEHAGroup(final String groupName,
                                             final int timeoutMs,
                                             final Logger logger,
                                             final Set<InetSocketAddress>
                                                 helperSockets,
                                             final ReplicationNetworkConfig
                                                 repNetConfig) {


        /* The timeout for the repeat check is min(total timeout, 1 second) */
        int checkMs = timeoutMs;
        if (checkMs > 1000) {
            checkMs = 1000;
        }

        final StringBuilder problem = new StringBuilder();
        final Set<ReplicationNode> groupDB = new HashSet<ReplicationNode>();
        boolean found =
            new PollCondition(checkMs, timeoutMs) {
                @Override
                protected boolean condition() {
                    logger.fine("TopoCheckUtils:  getting JE repGroupDb for " +
                                groupName);
                    try {
                        ReplicationGroupAdmin jeRGA =
                                new ReplicationGroupAdmin(groupName,
                                                          helperSockets,
                                                          repNetConfig);
                        groupDB.addAll(jeRGA.getGroup().getDataNodes());
                        groupDB.addAll(jeRGA.getGroup().getArbiterNodes());
                        return true;
                    } catch (Exception e) {
                        problem.append(e.getMessage());
                    }
                    return false;
                }
            }.await();

       if (!found) {
           logger.fine("TopoCheckUtils unable to get JE repGroupDb for " +
                       groupName + " " + problem);
       }

       return groupDB;
    }

    /**
     * Translate a hostname/port, which is how a JE HA node is identified,
     * into a SN id. Hostname is not sufficient, because multiple SNs may
     * be running on the same hostname.
     *
     * @param snCheckSet is the set of SNs that are mostly likely to match.
     * This is just an optimization; if none of the SNs there match, the method
     * looks through the entire topology.
     *
     * @return null if no SN is found
     */
    static StorageNodeId translateToSNId(Topology topo,
                                         Parameters params,
                                         Set<StorageNodeId> snCheckSet,
                                         String haHostname,
                                         int haPort) {
        /*
         * First look in the set of SNs that this shard is supposed to occupy
         */
        for (StorageNodeId snId: snCheckSet) {
            StorageNodeParams snp = params.get(snId);
            if (snp.getHAHostname().equals(haHostname)) {
                if (PortRange.contains(snp.getHAPortRange(), haPort)) {
                    return snId;
                }
            }
        }

        /*
         * Hmm, still didn't find the SN, we're going to have to look through
         * the whole topo.
         */
        for (StorageNodeId snId: topo.getStorageNodeIds()) {
            StorageNodeParams snp = params.get(snId);
            if (snp.getHAHostname().equals(haHostname)) {
                if (PortRange.contains(snp.getHAPortRange(), haPort)) {
                    return snId;
                }
            }
        }

        /*
         * Unexpected that we couldn't find a SN that match this host/port
         * pair. The caller should handle!
         */
        return null;
    }

    /**
     * Struct holding resource Ids owned by one storage node. Just a way to
     * peruse topology in another fashion.
     */
    static class SNServices {
        private final StorageNodeId snId;
        public final Set<RepNodeId> rnIds;
        public final Set<ArbNodeId> anIds;
        public AdminId adminId;
        public LoadParameters remoteParams;

        SNServices(StorageNodeId snId) {
            this.snId = snId;
            rnIds = new TreeSet<RepNodeId>();
            anIds = new TreeSet<ArbNodeId>();
        }

        public void add(AdminId adId) {
            this.adminId = adId;
        }

        public void add(RepNodeId rnId) {
            rnIds.add(rnId);
        }

        public void add(ArbNodeId arbId) {
            anIds.add(arbId);
        }

        SNServices(StorageNodeId snId, Set<RepNodeId> allRNs,
                   Set<ArbNodeId> allARBs, AdminId aId,
                   LoadParameters remoteParams) {
            this.snId = snId;
            rnIds = allRNs;
            this.adminId = aId;
            this.remoteParams = remoteParams;
            anIds = allARBs;
        }

        Set<RepNodeId> getAllRepNodeIds() {
            return rnIds;
        }

        AdminId getAdminId() {
            return adminId;
        }

        StorageNodeId getStorageNodeId() {
            return snId;
        }

        Set<RepNodeId> getAllRNs() {
            return rnIds;
        }

        Set<ArbNodeId> getAllARBs() {
            return anIds;
        }

        public boolean contains(ResourceId resId) {
            if (resId.getType().isRepNode()) {
                return rnIds.contains(resId);
            }
            return anIds.contains(resId);
        }
    }
}
