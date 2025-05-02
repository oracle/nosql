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

import java.util.UUID;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.impl.MinJEVersionUnsupportedException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

/**
 * Used to edit the RepGroupDb entry of the replication group.  See
 * {@link ReplicationGroupAdmin#getMutableGroup() getMutableGroup}
 */
public class ReplicationMutableGroup extends ReplicationGroup {

    public ReplicationMutableGroup(RepGroupImpl repImpl) {
        super(repImpl);
    }

    /**
     * Returns the UUID of the group.
     * 
     * @return group id
     */
    public UUID getUUID() {
        return repGroupImpl.getUUID();
    }

    /**
     * Sets the UUID of the group, even if the group already has a valid UUID.
     * This should only ever be used to repair a corrupted RepGroupDB database,
     * as the UUID value is assumed to never change once the node joins the
     * replication group.
     */
    public void setUUID(UUID id) {
        repGroupImpl.setUUIDNoCheck(id);
    }

    /**
     * Returns version of the schema format.
     *
     * @return schema format version.
     */
    public int getFormatVersion() {
        return repGroupImpl.getFormatVersion();
    }

    /**
     * Set the version of the schema format used to serialize and deserialize
     * the node and group objects.
     *
     * @param version of the format schema.
     * @param force if true, change the value even if it is invalid
     * @throws IllegalArgumentException if force is false and the format
     * version is invalid
     */
    public void setFormatVersion(int version, boolean force) {
        if (force) {
            repGroupImpl.setFormatVersionNoCheck(version);
        } else {
            repGroupImpl.setFormatVersion(version);
        }
    }

    /**
     * Get the change version.
     * 
     * @return the change version.
     */
    public int getChangeVersion() {
        return repGroupImpl.getChangeVersion();
    }

    /**
     * Sets the change version.  The value of the change version is incremented
     * every time a change is made to the group or to one of the nodes in the
     * group.  If force is false, the value will only change
     * if the new value is consistent with the change version of the nodes in
     * the group. If force is true the value will change even if it is
     * inconsistent with the nodes.
     *
     * The change version value will be set to the highest node change value
     * in the group when adding the group to RepGroupDB.
     * 
     * @param version of the group.
     * @param force   the value to change even if it conflicts with other
     *                settings.  This will not prevent the value from being set
     *                to the highest node change value.
     * @throws IllegalArgumentException if there is a version mismatch.
     */
    public void setChangeVersion(int version, boolean force)
                    throws IllegalArgumentException {
        if (force) {
            repGroupImpl.setChangeVersionNoCheck(version);
        } else {
            repGroupImpl.setChangeVersion(version);
        }
    }

    /**
     * Returns the current node id.
     * 
     * @return current node id.
     */
    public int getNodeIdSequence() {
        return repGroupImpl.getNodeIdSequence();
    }

    /**
     * Sets the nodeIdSequence.  nodeIdSequence is the value of the id of the
     * next node added to the group. If force is false, sets the new value only
     * if it is not less than or equal to the ids of any of the nodes in the
     * group. Set force to true to set the value regardless.
     * 
     * The nodeIdSequence will be set to the highest node id in the group
     * when the group is inserted into RepGroupDB.
     * 
     * @param id    value to which to set the sequence node id.
     * @param force if true, set the new value even if it conflicts with other
     *              settings.  This will not prevent the value from being set
     *              to the highest node id in the group.
     * @throws IllegalArgumentException if the id is less than the current
     *  value or greater than the maximum value, and force was set to false.
     */
    public void setNodeIdSequence(int id, boolean force) {
        if (force) {
            repGroupImpl.setNodeIdSequenceNoCheck(id);
        } else {
            repGroupImpl.setNodeIdSequence(id);
        }
    }

    /**
     * Get the minimum JE version a node can have to join the group.
     * 
     * @return minimum supported JE version.
     */
    public JEVersion getMinimumJEVersion() {
        return repGroupImpl.getMinJEVersion();
    }

    /**
     * Sets the minimum JE version.  If force is false the value will only
     * change if it will not disqualify any of the nodes in the existing group.
     * If force is true the value will always be changed.
     * 
     * @param version of JE.
     * @param force if true, set the value even if it conflicts with other
     *                settings.
     * @throws MinJEVersionUnsupportedException if force is false and the
     * version is invalid
     */
    public void setMinimumJEVersion(JEVersion version, boolean force)
                    throws MinJEVersionUnsupportedException {
        if (force) {
            repGroupImpl.setMinJEVersionNoCheck(version);
        } else {
            repGroupImpl.setMinJEVersion(version);
        }
    }

    public RepGroupImpl getGroup() {
        return repGroupImpl;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReplicationMutableGroup)) {
            return false;
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return 43 + (47 * super.hashCode());
    }

    @Override
    public String toString() {
        return repGroupImpl.toString();
    }
}
