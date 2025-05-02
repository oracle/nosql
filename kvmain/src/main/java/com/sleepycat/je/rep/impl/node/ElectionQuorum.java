/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.impl.node;

import java.util.logging.Logger;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.arbitration.Arbiter;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * ElectionQuorum centralizes decision making about what constitutes a
 * successful election quorum and the definition of an authoritative master.
 */
public class ElectionQuorum {

    private final RepImpl repImpl;
    private final Logger logger;

    /*
     * If non-zero use this value to override the normal group size
     * calculations.
     */
    private volatile int electableGroupSizeOverride;

    public ElectionQuorum(RepImpl repImpl) {

        this.repImpl = repImpl;
        logger = LoggerUtils.getLogger(getClass());

        electableGroupSizeOverride = repImpl.getConfigManager().
            getInt(RepParams.ELECTABLE_GROUP_SIZE_OVERRIDE);
        if (electableGroupSizeOverride > 0) {
            LoggerUtils.warning(logger, repImpl,
                                "Electable group size override set to:" +
                                electableGroupSizeOverride);
        }
    }

    /** For unit testing */
    public ElectionQuorum() {
        repImpl = null;
        logger = null;
    }

    /*
     * Sets the override value for the Electable Group size.
     */
    public void setElectableGroupSizeOverride(int override) {
        if (electableGroupSizeOverride != override) {
            LoggerUtils.warning(logger, repImpl,
                                "Electable group size override changed to:" +
                                override);
        }
        this.electableGroupSizeOverride = override;
    }

    public int getElectableGroupSizeOverride() {
        return electableGroupSizeOverride;
    }

    /**
     * Predicate to determine whether we have a quorum based upon the quorum
     * policy.
     */
    public boolean haveQuorum(QuorumPolicy quorumPolicy, int votes) {
        return votes >= getElectionQuorumSize(quorumPolicy);
    }

    /**
     * Called when overriding the current authorized master behavior.
     */
    public boolean isAuthoritativeMasterOld(MasterStatus masterStatus,
                                         FeederManager feederManager) {
        if (!masterStatus.isGroupMaster()) {
            return false;
        }

        return (feederManager.activeReplicaCount() + 1) >=
            getElectionQuorumSize(QuorumPolicy.SIMPLE_MAJORITY);
    }

    /**
     * Return the number of nodes that are required to achieve consensus on the
     * election. Over time, this may evolve to be a more detailed description
     * than simply the size of the quorum. Instead, it may return the set of
     * possible voters.
     *
     * Special situations, like an active designated primary or an election
     * group override will change the default quorum size.
     *
     * @param quorumPolicy
     * @return the number of nodes required for a quorum
     */
    public int getElectionQuorumSize(QuorumPolicy quorumPolicy) {
        if (electableGroupSizeOverride > 0) {
            return quorumPolicy.quorumSize(electableGroupSizeOverride);
        }

        /*
         * If arbitration is active, check whether arbitration determines the
         * election group size.
         */
        RepNode repNode = repImpl.getRepNode();
        // Can happen in testing.
        if (repNode == null) {
            return 0;
        }
        Arbiter arbiter = repNode.getArbiter();
        if (arbiter.isApplicable(quorumPolicy)) {
            return arbiter.getElectionQuorumSize(quorumPolicy);
        }

        return quorumPolicy.quorumSize
            (repNode.getGroup().getElectableGroupSize());
    }

    /**
     * Return whether nodes of the specified type should participate in
     * elections.
     *
     * @param nodeType the node type
     * @return whether nodes of that type should participate in elections
     */
    public boolean nodeTypeParticipates(final NodeType nodeType) {

        /* Only electable nodes participate in elections */
        return nodeType.isElectable();
    }
}
