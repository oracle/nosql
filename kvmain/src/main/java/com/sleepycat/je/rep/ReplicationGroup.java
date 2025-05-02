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

package com.sleepycat.je.rep;

import java.util.HashSet;
import java.util.Set;

import com.sleepycat.je.rep.impl.RepGroupImpl;

/**
 * An administrative view of the collection of nodes that form the replication
 * group. Can be obtained from a {@link ReplicatedEnvironment} or a {@link
 * com.sleepycat.je.rep.util.ReplicationGroupAdmin}.
 */
public class ReplicationGroup {

    /* All methods delegate to the group implementation. */
    final RepGroupImpl repGroupImpl;

    /**
     * @hidden
     * For internal use only
     * Used to wrap the actual group object implementation.
     */
    public ReplicationGroup(RepGroupImpl repGroupImpl) {
        this.repGroupImpl = repGroupImpl;
    }

    /**
     * Returns the name associated with the group.
     *
     * @return the name of the replication group.
     */
    public String getName() {
        return repGroupImpl.getName();
    }

    /**
     * Returns the set of all nodes in the group. The return value includes
     * ELECTABLE and SECONDARY nodes.
     *
     * <p>Note that SECONDARY nodes will only be included in the result when
     * this method is called for a replicated environment that is the master.
     *
     * @return the set of all nodes
     * @see NodeType
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public Set<ReplicationNode> getNodes() {
        final Set<ReplicationNode> result = new HashSet<>();
        repGroupImpl.includeMembers(null, result);
        return result;
    }

    /**
     * Returns the subset of nodes in the group with replicated environments
     * that participate in elections and can become masters, ignoring node
     * priority. The return value includes ELECTABLE nodes, and excludes
     * SECONDARY nodes.
     *
     * @return the set of electable nodes
     * @see NodeType
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public Set<ReplicationNode> getElectableNodes() {
        final Set<ReplicationNode> result = new HashSet<>();
        repGroupImpl.includeElectableMembers(result);
        return result;
    }

    /**
     * Returns the subset of nodes in the group with replicated environments
     * that do not participate in elections and cannot become masters. The
     * return value includes SECONDARY nodes, and excludes ELECTABLE nodes.
     *
     * <p>Note that SECONDARY nodes will only be returned when this method is
     * called for a replicated environment that is the master.
     *
     * @return the set of secondary nodes
     * @see NodeType
     * @since 6.0
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public Set<ReplicationNode> getSecondaryNodes() {
        final Set<ReplicationNode> result = new HashSet<>();
        repGroupImpl.includeSecondaryMembers(result);
        return result;
    }

    /**
     * Returns the subset of nodes in the group that store replication data.
     * The return value includes all ELECTABLE and SECONDARY nodes.
     *
     * <p>Note that SECONDARY nodes will only be included in the result when
     * this method is called for a replicated environment that is the master.
     *
     * @return the set of data nodes
     * @see NodeType
     * @since 6.0
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public Set<ReplicationNode> getDataNodes() {
        final Set<ReplicationNode> result = new HashSet<>();
        repGroupImpl.includeDataMembers(result);
        return result;
    }

    /**
     * Returns the subset of nodes in the group that participates in elections
     * but does not have a copy of the data and cannot become a master.
     * The return value includes ARBITER nodes.
     *
     * @return the set of arbiter nodes
     * @see NodeType
     */
    public Set<ReplicationNode> getArbiterNodes() {
        final Set<ReplicationNode> result = new HashSet<>();
        repGroupImpl.includeArbiterMembers(result);
        return result;
    }

    /**
     * Get administrative information about a node by its node name.
     *
     * <p>Note that SECONDARY nodes will only be returned when this method is
     * called for a replicated environment that is the master.
     *
     * @param nodeName the node name to be used in the lookup
     *
     * @return an administrative view of the node associated with nodeName, or
     * null if there isn't such a node currently in the group
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public ReplicationNode getMember(String nodeName) {
        return repGroupImpl.getMember(nodeName);
    }

    /**
     * @hidden
     * Internal use only.
     *
     * Returns the underlying group implementation object.
     */
    public RepGroupImpl getRepGroupImpl() {
        return repGroupImpl;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReplicationGroup)) {
            return false;
        }
        final ReplicationGroup otherGroup = (ReplicationGroup) obj;
        return repGroupImpl.equals(otherGroup.getRepGroupImpl());
    }

    @Override
    public int hashCode() {
        return 37 + (41 * repGroupImpl.hashCode());
    }

    /**
     * Returns a formatted version of the information held in a
     * ReplicationGroup.
     */
    @Override
    public String toString() {
        return repGroupImpl.toString();
    }
}
