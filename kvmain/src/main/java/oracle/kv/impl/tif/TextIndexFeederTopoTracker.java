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

package oracle.kv.impl.tif;

import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Object to track the topology changes and make the text index feeder (TIF)
 * acts accordingly to the new topology.
 */
public class TextIndexFeederTopoTracker
    implements TopologyManager.PostUpdateListener {

    private final RepNode repNode;
    private final TextIndexFeederManager tifm;
    private final Logger logger;

    public TextIndexFeederTopoTracker(RepNode repNode,
                                      TextIndexFeederManager tifm,
                                      Logger logger) {
        this.repNode = repNode;
        this.tifm = tifm;
        this.logger = logger;
    }

    @Override
    public boolean postUpdate(Topology topology) {

        TextIndexFeeder tif = tifm.getTextIndexFeeder();
        if (tif == null) {
            logger.log(Level.FINE,
                       lm("TIF unavailable, skip topology update with seq# " +
                          "{0}"),
                       topology.getSequenceNumber());
            /* Keeps this listener */
            return false;
        }

        final long newTopoSeq = topology.getSequenceNumber();
        final long currentTopoSeq = tif.getSubManager().getCurrentTopologySeq();

        /* skip update if not newer */
        if (newTopoSeq <= currentTopoSeq) {
            logger.log(Level.FINE,
                       lm("Ignore this update because new topology (seq: {0})" +
                          " is not newer than current (seq: {1})"),
                       new Object[]{newTopoSeq, currentTopoSeq});
            /* Keeps this listener */
            return false;
        }

        /* validate env since we may need to schedule new partition reader */
        if (repNode.getEnv(1) == null) {
            throw new OperationFaultException("Could not obtain env handle");
        }

        /* compute incoming and outbound partitions */
        final RepNodeId repNodeId = repNode.getRepNodeId();
        final RepGroupId repGroupId = topology.get(repNodeId).getRepGroupId();

        /* checked tif at the beginning of postUpdate() but if it is gone */
        tif = tifm.getTextIndexFeeder();
        if (tif == null) {
            return false;
        }

        synchronized (tif) {
            final Set<PartitionId> outPids =
                processOutgoingPartitions(repGroupId, tif, topology);
            final Set<PartitionId> inPids =
                processIncomingPartitions(repGroupId, tif, topology);

            tif.getSubManager().setCurrentTopologySeq(newTopoSeq);

            if (!outPids.isEmpty() || !inPids.isEmpty()) {
                logger.log(Level.INFO,
                           lm("TopologyTracker for TIF on RN {0} finished " +
                           "processing topology update seq # {1}.\n" +
                           "Outgoing partitions: " +
                           "{2}\nIncoming partitions: {3}"),
                           new Object[]{repNodeId.getFullName(), newTopoSeq,
                               Arrays.toString(outPids.toArray()),
                               Arrays.toString(inPids.toArray())});
            } else {
                logger.log(Level.FINE,
                           lm("TopologyTracker for TIF on RN {0} finished " +
                           "processing topology update seq # {1}, while no" +
                           " partition leaves or joins the group"),
                           new Object[]{repNodeId.getFullName(), newTopoSeq});
            }
        }

        /* Keeps this listener */
        return false;
    }

    /* Processes outgoing partitions due to topology change */
    private Set<PartitionId> processOutgoingPartitions(RepGroupId repGroupId,
                                                       TextIndexFeeder tif,
                                                       Topology topo) {

        final long topoSeq = topo.getSequenceNumber();
        final Set<PartitionId> outgoingParts = new HashSet<>();
        final PartitionMap newPartMap = topo.getPartitionMap();

        for (PartitionId pid : tif.getSubManager().getManagedPartitions()) {
            if (!repGroupId.equals(newPartMap.getRepGroupId(pid))) {
                outgoingParts.add(pid);
            }
        }

        if (!outgoingParts.isEmpty()) {
            for (PartitionId pid : outgoingParts) {
                tif.removePartition(pid);
            }

            logger.log(Level.FINE,
                       lm("Under topology seq# {0} all outgoing partitions " +
                          "have been processed: {1}"),
                       new Object[]{topoSeq,
                           Arrays.toString(outgoingParts.toArray())});

        }

        return outgoingParts;
    }

    /* Processes incoming partitions due to topology change */
    private Set<PartitionId>  processIncomingPartitions(RepGroupId repGroupId,
                                                        TextIndexFeeder tif,
                                                        Topology topo) {

        final long topoSeq = topo.getSequenceNumber();
        final Set<PartitionId> incomingParts = new HashSet<>();
        final Set<PartitionId> partitions = getPartitions(repGroupId, topo);

        for (PartitionId pid : partitions) {
            if (!tif.isManangedPartition(pid)) {
                incomingParts.add(pid);
            } else {
                logger.log(Level.FINE,
                           lm("Existing partition {0} owned by rep group {1}," +
                              " just ignore."),
                           new Object[]{pid, repGroupId});
            }
        }

        if (!incomingParts.isEmpty()) {
            for (PartitionId pid : incomingParts) {
                tif.addPartition(pid);
            }
            logger.log(Level.FINE,
                       lm("Under topology seq# {0} all incoming partitions " +
                          "processed: {1}"),
                       new Object[]{topoSeq,
                           Arrays.toString(incomingParts.toArray())});
        }

        return incomingParts;
    }

    /* Gets list of partitions belongs to the rep group in topology */
    private Set<PartitionId> getPartitions(RepGroupId repGroupId,
                                           Topology topo) {

        final Set<PartitionId> ret = new HashSet<>();

        /* get all partitions belongs to my group */
        for (PartitionId pid : topo.getPartitionMap().getAllIds()) {
            if (repGroupId.equals(topo.getRepGroupId(pid))) {
                ret.add(pid);
            }
        }

        logger.log(Level.FINE,
                   lm("Under topology seq# {0}, all partitions owned by " +
                   "replication group {1}: {2}."),
                   new Object[]{topo.getSequenceNumber(), repGroupId,
                       Arrays.toString(ret.toArray())});
        return ret;
    }

    private String lm(String s) {
        return "[tif][topo-tracker]" + s;
    }
}

