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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Utilities for use with TopologyBuilder and
 * Rules with respect to Arbiters.
 */
public class ArbiterTopoUtils {

    /**
     * Returns the Arbiter DC.
     * @param topo - topology
     * @param params - topology parameters
     * @return Arbiter datacenter Identifier
     */
    public static DatacenterId getArbiterDC(Topology topo,
                                            Parameters params) {
        DatacenterId anDC = getExistingArbiterDCId(topo, params);
        if (anDC == null ) {
            anDC = getBestArbiterDC(topo, params);
        }
        return anDC;
    }

    /**
     * Compute the number of SN's for a given DC that
     * can host ANs.
     *
     * @param topo
     * @param params
     * @param dcId
     * @return number of SNs
     */
    public static int getNumUsableSNs(Topology topo,
                                       Parameters params,
                                       DatacenterId dcId) {
        int numUsableSN = 0;
        for (StorageNode sn : topo.getStorageNodes(dcId)) {
            StorageNodeParams snp = params.get(sn.getResourceId());
            /* Don't count nodes that cannot host  AN's */
            if (!snp.getAllowArbiters()) {
                continue;
            }
            numUsableSN++;
        }
        return numUsableSN;
    }

    /**
     * Compute maximum number of ANs that should be present on any SN in the
     * given datacenter if the AN distribution is even.
     *
     * @param topo
     * @param params
     * @param dcId
     * @return average of AN's across the AN hosting SNs.
     */
    public static int computeZoneMaxANsPerSN(Topology topo,
                                   Parameters params,
                                   DatacenterId dcId) {
        int nShards = topo.getRepGroupIds().size();
        int numUsableSNs = getNumUsableSNs(topo, params, dcId);
        if (numUsableSNs == 0) {
            return 0;
        }

        /*
         * Given an RF=2 zone, each shard needs an arbiter.  If the number of
         * shards is a multiple of the number of SNs, then each SN should have
         * exactly that number of ANs.  If the number is not a multiple, then
         * we need at most one more AN in one or more of the SNs.
         */

        if (nShards % numUsableSNs == 0) {
            return nShards / numUsableSNs;
        }
        return nShards / numUsableSNs + 1;
    }

    /**
     * Method used to determine if Arbiters should be used
     * for the given topology. The total primary RF must be two
     * and there is a primary DC that allows hosting of ANs.
     *
     * @param topo
     * @return true if Arbiters should be used otherwise false.
     */
    public static boolean useArbiters(Topology topo) {

        /*
         *  Find the primary DC rep factor. TopologyBuilder takes into
         *  account Datacenters that have SN's. We do the same here.
         */
        int primaryRF = 0;
        final Set<DatacenterId> dcs = new HashSet<DatacenterId>();
        for (final StorageNodeId snId : topo.getStorageNodeIds()) {
            final DatacenterId dcId = topo.get(snId).getDatacenterId();
            if (dcs.add(dcId)) {
                final Datacenter dc = topo.get(dcId);

                /*
                 * Arbiters are allowed in primary zones only. This check
                 * is made just in case a secondary zone has allow arbiter set.
                 * This check should not be needed since you cannot create or
                 * alter a zone to secondary with allow arbiters set to true.
                 */
                if (dc.getDatacenterType().isPrimary()) {
                    primaryRF += topo.get(dcId).getRepFactor();
                }
            }
        }

        if (primaryRF == 2) {
            for (Datacenter dc : topo.getDatacenterMap().getAll()) {
                if (dc.getAllowArbiters()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Find the DC that contains SNs to host ANs.
     * If multiple DCs are configured to host ANs choose
     * based on priority given to RF zero, the number of SN's,
     * current hosting DC, or the DC created first.
     *
     * @param topo
     * @param params
     * @return datacenter id to host AN's or null.
     */
    public static DatacenterId getBestArbiterDC(Topology topo,
                                                 Parameters params) {

        Map<DatacenterId, Integer> snPerDC =
            getDCUsableSNsMap(topo, params);
        DatacenterId curDCId = getExistingArbiterDCId(topo, params);

        Datacenter arbDC = null;

        for (DatacenterId dcId : snPerDC.keySet()) {
            Datacenter tmpDc = topo.get(dcId);
            if (!tmpDc.getAllowArbiters()) {
                continue;
            }
            if (arbDC == null) {
                arbDC = topo.get(dcId);
                continue;
            }
            if (compare(arbDC, snPerDC.get(arbDC.getResourceId()),
                        topo.get(dcId), snPerDC.get(dcId),
                        topo, params, curDCId) > 0) {
                arbDC = topo.get(dcId);
            }
        }
        return arbDC == null ? null : arbDC.getResourceId();
    }

    /**
     * Choose the best DC from the set of DCs that currently
     * hosting Arbiters.
     */
    private static DatacenterId getExistingArbiterDCId(Topology topo,
                                                       Parameters params) {
        /*
         * Set of all DC's which contain arbiters
         */
        Set<DatacenterId> dcsWithArbiters = new HashSet<DatacenterId>();
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            dcsWithArbiters.add(topo.getDatacenterId(arbNodeId));
        }

        Map<DatacenterId, Integer> snPerDC =
            getDCUsableSNsMap(topo, params);

        DatacenterId arbDcId = null;

        for (DatacenterId dcId : dcsWithArbiters) {
            Datacenter dc = topo.get(dcId);

            /* Select only primary arbiter hosting datacenters */
            if (!dc.getAllowArbiters() &&
                    !dc.getDatacenterType().isPrimary()) {
                continue;
            }

            if (arbDcId == null) {
                arbDcId = dcId;
                continue;
            }

            if (compare(topo.get(arbDcId),
                        snPerDC.get(arbDcId),
                        topo.get(dcId), snPerDC.get(dcId),
                        topo, params, null) > 0) {
                arbDcId = dcId;
            }
        }
        return arbDcId;
    }

    /**
     * Given two Datacenters configured for arbiter's choose the better one
     * to host Arbiters.
     * Prefer DC with RF = 0 (with at least 1 AN hosting SN) to RF != 0
     * Otherwise, chose the datacenter with the larger number of SNs that
     * allow arbiters
     * Otherwise, chose the datacenter with the larger number of SNs that
     * allow arbiters or RNs, because the SNs to hold RNs make it easier to
     * obey arbiter proximity rules
     * Otherwise, choose the datacenter if it currently hosts arbiters.
     * Otherwise, choose the datacenter with the lower DC ID. This is done
     * so make sure the order is deterministic.
     *
     * @param dc1
     * @param numUsableSN1
     * @param dc2
     * @param numUsableSN2
     * @param topo
     * @param params
     * @return -1 dc1 better, 1 dc2 better
     */
    private static int compare(Datacenter dc1,
                               int numUsableSN1,
                               Datacenter dc2,
                               int numUsableSN2,
                               Topology topo,
                               Parameters params,
                               DatacenterId currentHostDCId) {

        /* check if one DC has zero RF the other doesn't */
        if (dc1.getRepFactor() == 0 && numUsableSN1 > 0 &&
            dc2.getRepFactor() > 0) {
            return -1;
        }
        if (dc2.getRepFactor() == 0 && numUsableSN2 > 0 &&
            dc1.getRepFactor() > 0) {
            return 1;
        }

        /* Choose DC with more SNs that allow arbiters */
        final DatacenterId dc1Id = dc1.getResourceId();
        final DatacenterId dc2Id = dc2.getResourceId();
        final int dc1ArbHostSns = countArbHostingSns(dc1Id, topo, params);
        final int dc2ArbHostSns = countArbHostingSns(dc2Id, topo, params);
        if (dc1ArbHostSns != dc2ArbHostSns) {
            return -Integer.signum(dc1ArbHostSns - dc2ArbHostSns);
        }

        /* Choose DC with more SNs that allow arbiters or RNs */
        if (numUsableSN1 != numUsableSN2) {
            return -Integer.signum(numUsableSN1 - numUsableSN2);
        }

        final int dc1Idnum = dc1.getResourceId().getDatacenterId();
        final int dc2Idnum = dc2.getResourceId().getDatacenterId();
        /* Choose if one of the datacenters hosts arbiters already. */
        if (currentHostDCId != null &&
            currentHostDCId.getDatacenterId() == dc1Idnum) {
            return -1;
        }

        if (currentHostDCId != null &&
            currentHostDCId.getDatacenterId() == dc2Idnum) {
            return 1;
        }

        /* Choose DC with lower ID */
        return Integer.signum(dc1Idnum - dc2Idnum);
    }

    /**
     * Return the number of SNs that can host ANs in the given
     * DC.
     *
     * @param dcId
     * @param topo
     * @param params
     * @return count of number of arb hosting SNs in dcId
     */
    private static int countArbHostingSns(DatacenterId dcId,
                                          Topology topo,
                                          Parameters params) {
        int arbHostingSns = 0;
        for (StorageNode sn : topo.getStorageNodes(dcId)) {
            StorageNodeId snId = sn.getResourceId();
            if (params.get(snId).getAllowArbiters()) {
                arbHostingSns++;
            }
        }
        return arbHostingSns;
    }

    /**
     * Create map of DCs that can host ANs to the number of
     * usable SNs in that DC.
     * @param topo
     * @return Map containing total number of usable SNs per datacenter.
     */
    private static Map<DatacenterId, Integer>
        getDCUsableSNsMap(Topology topo, Parameters params) {

        Map<DatacenterId, Integer> snPerDC =
            new HashMap<DatacenterId, Integer>();
        for (Datacenter dc : topo.getDatacenterMap().getAll()) {
            /* Check if the DC can host ANs */
            if (dc.getAllowArbiters() &&
                dc.getDatacenterType().isPrimary()) {
                DatacenterId dcId = dc.getResourceId();
                snPerDC.put(dcId, getNumUsableSNs(topo, params, dcId));
            }
        }
        return snPerDC;
    }
}
