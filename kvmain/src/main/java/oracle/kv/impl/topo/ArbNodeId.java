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

package oracle.kv.impl.topo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The KV Store wide unique resource id identifying a ARB_NODE in the KV Store.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ArbNodeId extends ServiceResourceId
        implements Comparable<ArbNodeId> {

    private static final long serialVersionUID = 1L;

    private static final String AR_PREFIX = "an";

    /* The store-wide unique group id. */
    private final int groupId;

    /* The group-wide unique node number. */
    private final int nodeNum;

    /**
     * The store-wide unique node id is constructed from the store-wide unique
     * group id and the group-wide unique node id.
     *
     * @param groupId the store-wide unique group id
     * @param nodeNum group-wide unique node number
     */
    public ArbNodeId(int groupId, int nodeNum) {
        super();
        this.groupId = groupId;
        this.nodeNum = nodeNum;
    }

    public static String getPrefix() {
        return AR_PREFIX;
    }

    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    ArbNodeId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        groupId = in.readInt();
        nodeNum = in.readInt();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getGroupId groupId}
     * <li> ({@link DataOutput#writeInt int}) {@link #getNodeNum nodeNum}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(groupId);
        out.writeInt(nodeNum);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.ARB_NODE;
    }

    @Override
    public int getGroupId() {
        return groupId;
    }

    @Override
    public int getNodeNum() {
        return nodeNum;
    }

    /**
     * Returns a string representation that uniquely identifies this node.
     * The fully qualified name contains both the group ID and the node
     * number.
     *
     * @return the fully qualified name of the ArbNode
     */
    @Override
    public String getFullName() {
        return new RepGroupId(getGroupId()).getGroupName() +
               "-" + AR_PREFIX + getNodeNum();
    }

    /**
     * Parses the fullName of a AN into its id.It accepts strings that are
     * in the format of {@link #getFullName()}.
     */
    public static ArbNodeId parse(String fullName) {
        String idArgs[] = fullName.split("-");
        if (idArgs.length == 2) {
            RepGroupId rgId = RepGroupId.parse(idArgs[0]);
            final int nodeNum = parseForInt(AR_PREFIX, idArgs[1]);
            return new ArbNodeId(rgId.getGroupId(), nodeNum);
        }

        throw new IllegalArgumentException
            (fullName +
             " is not a valid ArbNode id. It must follow the format rgX-anY");
    }

    /**
     * Returns just the name of the group portion of this ArbNode.  This
     * name is suitable for use as a BDB/JE HA Group name.
     *
     * @return the group name
     */
    public String getGroupName() {
        return new RepGroupId(getGroupId()).getGroupName();
    }

    @Override
    public String toString() {
        return getFullName();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public ArbNode getComponent(Topology topology) {
        return topology.get(this);
    }

    @Override
    protected ArbNode readComponent(Topology topology,
                                    DataInput in,
                                    short serialVersion)
        throws IOException {

        return new ArbNode(topology, this, in, serialVersion);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArbNodeId other = (ArbNodeId) obj;
        if (groupId != other.groupId) {
            return false;
        }
        if (nodeNum != other.nodeNum) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 41;
        int result = 1;
        result = prime * result + groupId;
        result = prime * result + nodeNum;
        return result;
    }

    @Override
    public int compareTo(ArbNodeId other) {
        int grp = getGroupId() - other.getGroupId();
        if (grp != 0) {
            return grp;
        }
        return getNodeNum() - other.getNodeNum();
    }

    @Override
    public ArbNodeId clone() {
        return new ArbNodeId(this.groupId, this.nodeNum);
    }
}
