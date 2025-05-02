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
 * The KV Store wide unique resource id identifying a group.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RepGroupId extends ResourceId implements Comparable<RepGroupId> {

    public static RepGroupId NULL_ID = new RepGroupId(-1);

    /**
     * The prefix used for rep group names
     */
    private static final String REP_GROUP_PREFIX = "rg";

    private static final long serialVersionUID = 1L;

    /* The store-wide unique group id. */
    private final int groupId;

    /**
     * The unique group id, as determined by the GAT, that's used as the basis
     * of the resourceId. If the group id is negative, that means it represents
     * a shard id created internally for special purpose, either for the
     * NULL_ID or see {@link oracle.kv.impl.pubsub.CheckpointTableManager}.
     *
     * @param groupId the unique groupId
     */
    public RepGroupId(int groupId) {
        this.groupId = groupId;
    }

    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    RepGroupId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        groupId = in.readInt();
    }

    public boolean isNull() {
        return groupId == NULL_ID.groupId;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getGroupId groupId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(groupId);
    }

    public static String getPrefix() {
        return REP_GROUP_PREFIX;
    }

    @Override
    public ResourceType getType() {
        return ResourceType.REP_GROUP;
    }

    @Override
    public String getFullName() {
        return getGroupName();
    }

    /**
     * Returns the group-wide unique group id.
     */
    public int getGroupId() {
        return groupId;
    }

    /**
     * Returns the name of the RepGroup. This name is suitable for use as a
     * BDB/JE HA Group name.
     *
     * @return the group name
     */
    public String getGroupName() {
        return REP_GROUP_PREFIX + getGroupId();
    }

    public boolean sameGroup(RepNodeId repNodeId) {
        return (groupId == repNodeId.getGroupId());
    }

    /**
     * Generates a repGroupId from a group name. It's the inverse of
     * {@link #getGroupName}.
     */
    public static RepGroupId parse(String groupName) {
        return new RepGroupId(parseForInt(REP_GROUP_PREFIX, groupName));
    }

    @Override
    public String toString() {
        return getGroupName();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public RepGroup getComponent(Topology topology) {
        return topology.get(this);
    }

    @Override
    protected RepGroup readComponent(Topology topology,
                                     DataInput in,
                                     short serialVersion)
        throws IOException {

        return new RepGroup(topology, this, in, serialVersion);
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
        RepGroupId other = (RepGroupId) obj;
        if (groupId != other.groupId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + groupId;
        return result;
    }

    @Override
    public int compareTo(RepGroupId other) {
        return getGroupId() - other.getGroupId();
    }

    @Override
    public RepGroupId clone() {
        return new RepGroupId(this.groupId);
    }

    public static RepGroupId getRepGroupId(ResourceId resId) {
        if (resId instanceof RepGroupId) {
            return new RepGroupId(((RepGroupId)resId).getGroupId());
        }
        return null;
    }
}
