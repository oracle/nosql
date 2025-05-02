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
package com.sleepycat.je.rep.stream;

import java.net.InetSocketAddress;

import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.NameIdPair;

/**
 * Class used by a node to track changes in Master Status. It's updated by
 * the Listener. It represents the abstract notion that the notion of the
 * current Replica Group is definitive and is always in advance of the notion
 * of a master at each node. A node is typically playing catch up as it tries
 * to bring its view in line with that of the group.
 */
public class MasterStatus implements Cloneable {

    /* This node's identity */
    private final NameIdPair nameIdPair;

    /* The current master resulting from election notifications */
    private String groupMasterHostName = null;
    private int groupMasterPort = 0;
    /* The node ID used to identify the master. */
    private NameIdPair groupMasterNameId = NameIdPair.NULL;
    /* The proposal that resulted in this group master. */
    private Proposal groupMasterProposal = null;

    /*
     * The Master as implemented by the Node. It can lag the groupMaster
     * as the node tries to catch up.
     */
    private String nodeMasterHostName = null;
    private int nodeMasterPort = 0;
    private NameIdPair nodeMasterNameId = NameIdPair.NULL;
    /* The proposal that resulted in this node becoming master. */
    private Proposal nodeMasterProposal = null;

    /*
     * True, if the node knows of a master (could be out of date) and the node
     * and group's view of the master is in sync. The two are out of sync when
     * a node is informed about the result of an election, but has not had a
     * chance to react to that election result. Make sure to update this field
     * by calling updateInSync if nodeMasterNameId or groupMasterNameId change.
     */
    private volatile boolean inSync = false;

    public MasterStatus(NameIdPair nameIdPair) {
        this.nameIdPair = nameIdPair;
    }

    /**
     * Returns a read-only snapshot of the object.
     */
    @Override
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            assert(false);
        }
        return null;
    }

    /**
     * Returns true if it's the master from the Group's perspective
     */
    public synchronized boolean isGroupMaster() {
        final int id = nameIdPair.getId();
        return (id != NameIdPair.NULL_NODE_ID) &&
            (id == groupMasterNameId.getId());
    }

    /**
     * Returns true if it's the master from the node's localized perspective
     */
    public synchronized boolean isNodeMaster() {
        final int id = nameIdPair.getId();
        return (id != NameIdPair.NULL_NODE_ID) &&
            (id == nodeMasterNameId.getId());
    }

    /**
     * Sets the group master and returns {@code true} if the group and node
     * still have a consistent notion of the master.
     *
     * We still have a consistent notion of the master if we did not change the
     * master but only the proposal number is bumped.
     */
    public synchronized boolean setGroupMaster(String hostname,
                                               int port,
                                               NameIdPair newGroupMasterNameId,
                                               Proposal proposal) {
        groupMasterHostName = hostname;
        groupMasterPort = port;
        groupMasterNameId = newGroupMasterNameId;
        groupMasterProposal = proposal;

        /*
         * Sync directly to avoid breaking master connections.
         *
         * In both Feeder and Replica loops, we check with assertSync and
         * breaks if there is a new learned master. Optimally, we should not
         * break those loops when the master stays the same and there is only a
         * term change.
         *
         * It is safe to sync directly as long as we ensure the same side
         * effects with the other sync routine which breaks the loop. The only
         * side effect is bumping the term for NodeState. This is done in the
         * MasterChangeListener.
         */
        if (((nodeMasterHostName != null)
             && nodeMasterHostName.equals(groupMasterHostName))
            && (nodeMasterPort == groupMasterPort)
            && ((nodeMasterNameId != null)
                && nodeMasterNameId.equals(groupMasterNameId))) {
            return sync();
        }


        return updateInSync();
    }

    /**
     * Predicate to determine whether the group and node have a consistent
     * notion of the Master. Note that it's not synchronized to minimize
     * contention.
     *
     * @return false if the node does not know of a Master, or the group Master
     * is different from the node's notion the master.
     */

    public boolean inSync() {
        return inSync;
    }

    /**
     * Updates the consistent notion of the Master.
     *
     * @return {@code true} if in sync
     */
    private boolean updateInSync() {
        assert Thread.holdsLock(this);
        inSync = !nodeMasterNameId.hasNullId() &&
            (isNodeMaster() ?
                /*
                 * A master, avoid unnecessary transitions through the
                 * UNKNOWN state, due to new re-affirming proposals
                 */
             (groupMasterNameId.getId() == nodeMasterNameId.getId()) :
                 /*
                  * A replica, need to reconnect, if there is a newer proposal
                  */
             (groupMasterProposal == nodeMasterProposal));
        return inSync;
    }

    public synchronized void unSync() {
        nodeMasterHostName = null;
        nodeMasterPort = 0;
        nodeMasterNameId = NameIdPair.NULL;
        nodeMasterProposal = null;
        updateInSync();
    }

    /**
     * An assertion form of the above. By combining the check and exception
     * generation in an atomic operation, it provides for an accurate exception
     * message.
     *
     * @throws MasterObsoleteException
     */
    public void assertSync()
        throws MasterObsoleteException {

        if (!inSync()) {
            synchronized (this) {
                /* Check again in synchronized block. */
                if (!inSync()) {
                    throw new MasterObsoleteException(this);
                }
            }
        }
    }

    /**
     * Syncs to the group master
     *
     * @return {@code true} if in sync
     */
    public synchronized boolean sync() {
        nodeMasterHostName = groupMasterHostName;
        nodeMasterPort = groupMasterPort;
        nodeMasterNameId = groupMasterNameId;
        nodeMasterProposal = groupMasterProposal;
        return updateInSync();
    }

    /**
     * Returns the Node's current idea of the Master. It may be "out of sync"
     * with the Group's notion of the Master
     */
    public synchronized InetSocketAddress getNodeMaster() {
        if (nodeMasterHostName == null) {
            return null;
        }
        return new InetSocketAddress(nodeMasterHostName, nodeMasterPort);
    }

    public synchronized NameIdPair getNodeMasterNameId() {
        return nodeMasterNameId;
    }

    public synchronized MasterIdTerm getNodeMasterIdTerm() {
        return new MasterIdTerm(nodeMasterNameId.getId(),
                                nodeMasterProposal.getTimeMs());
    }

    /**
     * Returns a socket that can be used to communicate with the group master.
     * It can return null, if there is no current group master, that is,
     * groupMasterNameId is NULL.
     */
    public synchronized InetSocketAddress getGroupMaster() {
        if (groupMasterHostName == null) {
             return null;
        }
        return new InetSocketAddress(groupMasterHostName, groupMasterPort);
    }

    public synchronized NameIdPair getGroupMasterNameId() {
        return groupMasterNameId;
    }

    public synchronized long getGroupMasterTerm() {
        return (groupMasterProposal == null)
            ? MasterTerm.MIN_TERM
            : groupMasterProposal.getTimeMs();
    }

    /**
     * Clears the group master so that we can force a new election.
     */
    public synchronized void clearGroupMaster() {
        setGroupMaster(null, 0, NameIdPair.NULL, null);
        unSync();
    }
}
