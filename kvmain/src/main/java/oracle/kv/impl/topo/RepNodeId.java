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
 * The KV Store wide unique resource id identifying a REP_NODE in the KV Store.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RepNodeId extends ServiceResourceId
        implements Comparable<RepNodeId> {

    private static final long serialVersionUID = 1L;

    private static final String RN_PREFIX = "rn";

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
    public RepNodeId(int groupId, int nodeNum) {
        super();
        this.groupId = groupId;
        this.nodeNum = nodeNum;
    }

    public static String getPrefix() {
        return RN_PREFIX;
    }

    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    RepNodeId(DataInput in, short serialVersion)
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
        return ResourceType.REP_NODE;
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
     * @return the fully qualified name of the RepNode
     */
    @Override
    public String getFullName() {
        return new RepGroupId(getGroupId()).getGroupName() +
               "-" + RN_PREFIX + getNodeNum();
    }

    /**
     * Parses the fullName of a RN into its id.It accepts strings that are
     * in the format of {@link #getFullName()}, and for backward compatibility,
     * also accepts &lt;groupNum,nodeNum&gt;.
     */
    public static RepNodeId parse(String fullName) {
        String idArgs[] = fullName.split("-");
        if (idArgs.length == 2) {
            RepGroupId rgId = RepGroupId.parse(idArgs[0]);
            final int nodeNum = parseForInt(RN_PREFIX, idArgs[1]);
            return new RepNodeId(rgId.getGroupId(), nodeNum);
        }

        /* backward compatibility for older groupNum,nodeNum syntax */
        idArgs = fullName.split(",");
        if (idArgs.length == 2) {
            try {
                return new RepNodeId(Integer.parseInt(idArgs[0]),
                                     Integer.parseInt(idArgs[1]));
            } catch(NumberFormatException e) {
                /* Fall through and throw IllegalArgEx */
            }
        }

        throw new IllegalArgumentException
            (fullName +
             " is not a valid RepNode id. It must follow the format rgX-rnY");
    }

    /**
     * Returns just the name of the group portion of this RepNode.  This
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
    public RepNode getComponent(Topology topology) {
        return topology.get(this);
    }

    @Override
    protected RepNode readComponent(Topology topology,
                                    DataInput in,
                                    short serialVersion)
        throws IOException {

        return new RepNode(topology, this, in, serialVersion);
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
        RepNodeId other = (RepNodeId) obj;
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
        final int prime = 31;
        int result = 1;
        result = prime * result + groupId;
        result = prime * result + nodeNum;
        return result;
    }

    @Override
    public int compareTo(RepNodeId other) {
        int grp = getGroupId() - other.getGroupId();
        if (grp != 0) {
            return grp;
        }
        return getNodeNum() - other.getNodeNum();
    }

    @Override
    public RepNodeId clone() {
        return new RepNodeId(this.groupId, this.nodeNum);
    }
}
