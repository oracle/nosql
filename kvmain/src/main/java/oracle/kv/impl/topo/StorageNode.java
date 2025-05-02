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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology.Component;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.SerializationUtil;

import oracle.nosql.common.json.ObjectNode;

/**
 * The SN topology component as created by the GAT and maintained by it.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class StorageNode extends Component<StorageNodeId>
    implements Comparable<StorageNode> {

    private static final long serialVersionUID = 1L;

    /**
     * The data center that hosts this SN.
     */
    private final DatacenterId datacenterId;

    /**
     * The hostname/interface used for KVS communications.
     */
    private final String hostname;

    /**
     * The registry port used by the RMI registry on this SN
     */
    private final int registryPort;

    public StorageNode(Datacenter datacenter,
                       String hostname,
                       int registryPort) {
        this(datacenter.getResourceId(), hostname, registryPort);
    }

    public StorageNode(DatacenterId datacenterId,
                       String hostname,
                       int registryPort) {
        this.datacenterId = datacenterId;
        this.hostname = hostname;
        this.registryPort = registryPort;
    }

    StorageNode(Topology topology,
                StorageNodeId snId,
                DataInput in,
                short serialVersion)
        throws IOException {

        super(topology, snId, in, serialVersion);

        if (in.readBoolean()) {
            final ResourceId rId =
                ResourceId.readFastExternal(in, serialVersion);
            if (!(rId instanceof DatacenterId)) {
                throw new IOException("Expected DatacenterId: " + rId);
            }
            datacenterId = (DatacenterId) rId;
        } else {
            datacenterId = null;
        }
        hostname = readString(in, serialVersion);
        registryPort = readPackedInt(in);
    }

    private StorageNode(StorageNode storageNode) {
        super(storageNode);
        datacenterId = storageNode.datacenterId.clone();
        hostname = storageNode.hostname;
        registryPort = storageNode.registryPort;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Component}) {@code super}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull DatacenterId or
     *      null}) {@link #getDatacenterId datacenterId}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getHostname
     *      hostname}
     * <li> ({@link SerializationUtil#writePackedInt packedInt}) {@link
     *      #getRegistryPort registryPort}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, datacenterId);
        writeString(out, serialVersion, hostname);
        writePackedInt(out, registryPort);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#getResourceType()
     */
    @Override
    public ResourceType getResourceType() {
        return ResourceType.STORAGE_NODE;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result +
            ((datacenterId == null) ? 0 : datacenterId.hashCode());
        result = prime * result +
            ((hostname == null) ? 0 : hostname.hashCode());
        result = prime * result + registryPort;
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

        return propertiesEquals((StorageNode) obj);
    }

    /**
     * @return true if this storage node already exists in the topology,
     * excluding any comparison of resource id and sequence number.
     */
    public boolean propertiesEquals(StorageNode other) {
        if (datacenterId == null) {
            if (other.datacenterId != null) {
                return false;
            }
        } else if (!datacenterId.equals(other.datacenterId)) {
            return false;
        }

        if (hostname == null) {
            if (other.hostname != null) {
                return false;
            }
        } else if (!hostname.equals(other.hostname)) {
            return false;
        }
        if (registryPort != other.registryPort) {
            return false;
        }
        return true;
    }

    /**
     * Returns the datacenter id for the datacenter hosting the storage node.
     */
    public DatacenterId getDatacenterId() {
        return datacenterId;
    }

    /**
     * Returns the hostname associated with the StorageNode.
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Returns the registry port associated with the SN.
     */
    public int getRegistryPort() {
        return registryPort;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#clone()
     */
    @Override
    public Component<?> clone() {
        return new StorageNode(this);
    }

    @Override
    public StorageNodeId getStorageNodeId() {
        return getResourceId();
    }

    @Override
    public boolean isMonitorEnabled() {
        return true;
    }

    @Override
    public String toString() {
        return "[" + getResourceId() + "] " +
            DatacenterId.DATACENTER_PREFIX + ":[id=" + datacenterId +
            " name=" + getTopology().get(datacenterId).getName() + "] " +
            hostname + ":" + registryPort;
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put("resourceId", getResourceId().toString());
        top.put("hostname", hostname);
        top.put("registryPort", registryPort);
        top.set("zone", getTopology().get(datacenterId).toJson());
        return top;
    }

    @Override
    public int compareTo(StorageNode other) {
        return getStorageNodeId().compareTo(other.getStorageNodeId());
    }
}
