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

import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology.Component;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.SerializationUtil;

import oracle.nosql.common.json.ObjectNode;

/**
 * An Entry in the PartitionMap
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Partition extends Topology.Component<PartitionId> {

    private static final long serialVersionUID = 1L;
    private final RepGroupId repGroupId;

    public Partition(RepGroup repGroup) {
        this(repGroup.getResourceId());
    }

    public Partition(RepGroupId repGroupId) {
        this.repGroupId = repGroupId;
    }

    private Partition(Partition partition) {
        super(partition);
        repGroupId = partition.repGroupId.clone();
    }

    Partition(Topology topology,
              PartitionId partitionId,
              DataInput in,
              short serialVersion)
        throws IOException {

        super(topology, partitionId, in, serialVersion);

        if (in.readBoolean()) {
            final ResourceId rId =
                ResourceId.readFastExternal(in, serialVersion);
            if (!(rId instanceof RepGroupId)) {
                throw new IOException("Expected RepGroupId: " + rId);
            }
            repGroupId = (RepGroupId) rId;
        } else {
            repGroupId = null;
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li>({@link ResourceId}) {@code super}
     * <li>({@link SerializationUtil#writeFastExternalOrNull RepGroupId or
     *     null}) {@link #getRepGroupId repGroupId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, repGroupId);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#getResourceType()
     */
    @Override
    public ResourceType getResourceType() {
        return ResourceType.PARTITION;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#clone()
     */
    @Override
    public Component<?> clone() {
        return new Partition(this);
    }

    /**
     * Returns the RepGroupId associated with the partition
     */
    public RepGroupId getRepGroupId() {
        return repGroupId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
            ((repGroupId == null) ? 0 : repGroupId.hashCode());
        return result;
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

        if (!super.equals(obj)) {
            return false;
        }

        Partition other = (Partition) obj;
        return propertiesEqual(other);
    }

    /**
     * @return true if the logical portion of the Partitions are equal.
     */
    public boolean propertiesEqual(Partition other) {

        if (repGroupId == null) {
            if (other.repGroupId != null) {
                return false;
            }
        } else if (!repGroupId.equals(other.repGroupId)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "[" + getResourceId() + "] " +  " shard=" +  repGroupId;
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put("resourceId", getResourceId().toString());
        top.put("shardId", repGroupId.toString());
        return top;
    }
}
