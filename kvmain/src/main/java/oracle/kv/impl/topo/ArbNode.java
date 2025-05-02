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
 * The AN topology component.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ArbNode extends Component<ArbNodeId>
    implements Comparable<ArbNode> {

    private static final long serialVersionUID = 1L;

    private final StorageNodeId storageNodeId;

    public ArbNode(StorageNodeId storageNodeId) {

        this.storageNodeId = storageNodeId;
    }

    private ArbNode(ArbNode arbNode) {
        super(arbNode);

        storageNodeId = arbNode.storageNodeId.clone();
    }

    ArbNode(Topology topology,
            ArbNodeId anId,
            DataInput in,
            short serialVersion)
        throws IOException {

        super(topology, anId, in, serialVersion);
        if (in.readBoolean()) {
            final ResourceId rId =
                ResourceId.readFastExternal(in, serialVersion);
            if (!(rId instanceof StorageNodeId)) {
                throw new IOException("Expected StorageNodeId: " + rId);
            }
            storageNodeId = (StorageNodeId) rId;
        } else {
            storageNodeId = null;
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Component}) {@code super}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull StorageNodeId or
     *      null}) {@link #getStorageNodeId storageNodeId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, storageNodeId);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#getResourceType()
     */
    @Override
    public ResourceType getResourceType() {
        return ResourceType.ARB_NODE;
    }

    @Override
    public int hashCode() {
        final int prime = 37;
        int result = super.hashCode();
        result = prime * result +
            ((storageNodeId == null) ? 0 : storageNodeId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArbNode other = (ArbNode) obj;
        return propertiesEquals(other);
    }

    public boolean propertiesEquals(ArbNode other) {

        if (storageNodeId == null) {
            if (other.storageNodeId != null) {
                return false;
            }
        } else if (!storageNodeId.equals(other.storageNodeId)) {
            return false;
        }
        return true;
    }


    /**
     * Returns the replication group id associated with the AN.
     */
    public RepGroupId getRepGroupId() {
        return new RepGroupId(getResourceId().getGroupId());
    }

    /**
     * Returns the StorageNodeId of the SN hosting this AN
     */
    @Override
    public StorageNodeId getStorageNodeId() {
        return storageNodeId;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#clone()
     */
    @Override
    public ArbNode clone() {
        return new ArbNode(this);
    }

    @Override
    public boolean isMonitorEnabled() {
        return true;
    }

    @Override
    public String toString() {
        return "[" + getResourceId() + "]" + " sn=" + storageNodeId;
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put("resourceId", getResourceId().toString());
        top.put("snId", storageNodeId.toString());
        return top;
    }

    @Override
    public int compareTo(ArbNode other) {
        return getResourceId().compareTo
            (other.getResourceId());
    }
}
