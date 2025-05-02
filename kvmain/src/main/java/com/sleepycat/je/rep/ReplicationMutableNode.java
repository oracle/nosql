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

import static com.sleepycat.je.rep.impl.node.NameIdPair.NULL_NODE_ID;

import java.net.InetSocketAddress;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.impl.RepGroupDB;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

/**
 * Used to edit the RepGroupDb entry of a replication node.  See
 * {@link ReplicationGroupAdmin#getNode(String) getNode}
 */
public class ReplicationMutableNode implements ReplicationNode {

    private RepNodeImpl node;

    private final RepGroupImpl group;

    /*
     * Contains the old node in case the node name or id is changed (this is so
     * the old node can be deleted from the database and removed from
     * RepGroupImpl.idToNode and RepGroupImpl.nameToNode).
     */
    private RepNodeImpl oldNode;

    /**
     * Constructor.
     * 
     * @param node this replication node
     * @param group of the replication node
     * @param newNode whether this is a new node, or is an existing node.
     */
    public ReplicationMutableNode(RepNodeImpl node,
                    RepGroupImpl group,
                    boolean newNode) {
        if (node == null || group == null) {
            throw new IllegalArgumentException(
                            "node and group must be non-null.");
        }
        this.node = node;
        this.group = group;
        if (!newNode) {
            setOldNode(node);
        }
    }

    public RepNodeImpl getOldNode() {
        return oldNode;
    }

    /**
     * Returns the node id.
     * 
     * @return node id
     */
    public int getId() {
        return node.getNodeId();
    }

    /**
     * Changes the id of the replication node.  If force is false then the
     * value will only be changed if it is not less than or equal to
     * RepGroupImpl.nodeIdSequence. If force is set to true then the value
     * will always be changed.
     *
     * Changing the id of an active node will result in an exception being
     * thrown when attempting to place the node back into the database.
     * 
     * @param id    of the replication node.
     * @param force if true, the new value will be used even if it is invalid
     * @throws IllegalArgumentException if force is false and the value is
     * invalid
     */
    public void setId(int id, boolean force) throws IllegalArgumentException {
        // Transient nodes can have the NULL_NODE_ID.
        if (!force && (id <= group.getNodeIdSequence() && id != NULL_NODE_ID)) {
            throw new IllegalArgumentException(
                "Id must be greater than the current maximum sequence id of " +
                group.getNodeIdSequence());
        }
        node.getNameIdPair().setId(id, false);
    }

    /**
     * Changes the name of the replication node. As a side effect, when this
     * node is placed back into the database, the RepGroupImpl.nodesById and
     * RepGroupImpl.nodesByName maps will be updated appropriately.
     * 
     * Changing the name of an active node will result in an exception being
     * thrown when attempting to place the node back into the database.
     *
     * @param name of the replication node.
     */
    public void setName(String name) {
        if (name == null || name.equals(RepGroupDB.GROUP_KEY)) {
            throw new IllegalArgumentException(
                "Node name cannot be null or " + RepGroupDB.GROUP_KEY);
        }
        node = new RepNodeImpl(new NameIdPair(name, node.getNodeId()),
                        node.getType(), node.isQuorumAck(), node.isRemoved(),
                        node.getHostName(), node.getPort(),
                         node.getChangeVersion(), node.getJEVersion());
    }

    @Override
    public String getName() {
        return node.getName();
    }

    @Override
    public NodeType getType() {
        return node.getType();
    }

    /**
     * Sets the node type if it does not conflict with the isRemoved value. Set
     * forced to true to change the value regardless. Sets the node ID to
     * NULL_NODE_ID if the type uses transient node IDs.
     * 
     * Changing the node type on an active node will cause an exception when
     * attempting to place the node in the database.
     * 
     * @param type  of the node.
     * @param force if true, the new value will be used even if it is invalid
     * @throws IllegalArgumentException if force is false and the value is
     * invalid
     */
    public void setType(NodeType type, boolean force)
                    throws IllegalArgumentException {
        if (!force && node.isRemovedNoCheck() && type.hasTransientId()) {
            throw new IllegalArgumentException(
                            "Cannot set a transient type to a node marked as removed.");
        }
        node.setType(type);
        if (type.hasTransientId()) {
            setId(NULL_NODE_ID, false);
        }
    }

    /**
     * Set the address of the node.
     * 
     * Changing the address of an active node will cause an exception when
     * attempting to place the node back into the database.
     * 
     * @param hostName of the node address.
     * @param port     of the node address.
     */
    public void setAddress(String hostName, int port) {
        node.setHostName(hostName);
        node.setPort(port);
    }

    /**
     * Return the quorumAck value.
     * 
     * @return quorumAck value
     */
    public boolean getQuorumAck() {
        return node.isQuorumAck();
    }

    /**
     * Set the quorumAck value.  quorumAck is true or false based on whether
     * the other nodes in the replication group have acknowledged its addition
     * to the group.
     * 
     * Note that when setting quorumAck to false, the value can be set to true
     * later when a quorum of nodes in the group acknowledge it, if it is of
     * a node type that requires acknowledgement.
     * 
     * @param quorumAck of the node.
     */
    public void setQuorumAck(boolean quorumAck) {
        node.setQuorumAck(quorumAck);
    }

    /**
     * Returns the value of isRemoved.
     * 
     * @return isRemove value
     */
    public boolean getIsRemoved() {
        return node.isRemovedNoCheck();
    }

    /**
     * Set the value of isRemove if it does not cause a conflict with the type
     * of the database.  isRemoved is true or false based on whether the node
     * has been removed from the replication group.
     * 
     * @param isRemoved to true or false depending on if the node has been
     *                  removed from the group.
     * @param force if true, the new value will be used even if it is invalid
     * @throws IllegalArgumentException if force is false and the value is
     * invalid
     */
    public void setIsRemoved(boolean isRemoved, boolean force)
                    throws IllegalArgumentException {
        if (!force && isRemoved && node.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                            "Cannot set isRemove to true on transient nodes.");
        }
        node.setRemoved(isRemoved);
    }

    /**
     * Gets the change version.
     * 
     * @return change version
     */
    public int getChangeVersion() {
        return node.getChangeVersion();
    }

    /**
     * Sets the change version if it does not conflict with the replication
     * group change version. Set force to true to force the change.
     * 
     * @param version of the current replication node.
     * @param force if true, the new value will be used even if it is invalid
     * @throws IllegalArgumentException if force is false and the value is
     * invalid
     */
    public void setChangeVersion(int version, boolean force)
                    throws IllegalArgumentException {
        if (!force && (version > group.getChangeVersion())) {
            throw new IllegalArgumentException(
                "Node version is greater than the group change version " +
                group.getChangeVersion());
        }
        node.setChangeVersion(version);
    }

    /**
     * Gets the JE version.
     * 
     * @return JE version
     */
    public JEVersion getJEVersion() {
        return node.getJEVersion();
    }

    /**
     * Sets the JE version unless doing so would invalidate the node because the
     * version is not supported by the replication group. Set force to true to
     * force the change.
     * 
     * @param version of JE that this node supports.
     * @param force if true, the new value will be used even if it is invalid
     * @throws IllegalArgumentException if force is false and the value is
     * invalid
     */
    public void setJEVersion(JEVersion version, boolean force)
                    throws IllegalArgumentException {
        if (!force && group.getMinJEVersion().compareTo(version) > 0) {
            throw new IllegalArgumentException(
                "New JE version is less than the minimal " +
                "acceptable JE version for the group: " +
                group.getMinJEVersion().getVersionString());
        }
        node.updateJEVersion(version);
    }

    public RepNodeImpl getNode() {
        return node;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(node.getHostName(), node.getPort());
    }

    @Override
    public String getHostName() {
        return node.getHostName();
    }

    @Override
    public int getPort() {
        return node.getPort();
    }

    @Override
    public String toString() {
        return node.toString();
    }

    @Override
    public boolean equals(Object obj) {
        ReplicationMutableNode otherNode;
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ReplicationMutableNode)) {
            return false;
        }
        otherNode = (ReplicationMutableNode) obj;
        if (!node.equals(otherNode.getNode())) {
            return false;
        }
        if (!group.equals(otherNode.group)) {
            return false;
        }
        if (oldNode != null && !oldNode.equals(otherNode.getOldNode())) {
            return false;
        }
        if (oldNode == null && otherNode.getOldNode() != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 43;
        result = result + node.hashCode();
        result = (47 * result) + group.hashCode();
        return result;
    }

    private void setOldNode(RepNodeImpl node) {
        oldNode = new RepNodeImpl(
                        new NameIdPair(node.getName(), node.getNodeId()),
                        node.getType(), node.isQuorumAck(), node.isRemoved(),
                        node.getHostName(), node.getPort(),
                        node.getChangeVersion(), node.getJEVersion());
    }

    /**
     * Clears the oldNode field, which requests that the node's old record be
     * deleted from RepGroupDB when adding the new record.
     */
    public void clearOldNode() {
        oldNode = null;
    }
}
