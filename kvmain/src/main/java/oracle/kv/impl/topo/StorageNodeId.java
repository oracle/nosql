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
 * @see #writeFastExternal FastExternalizable format
 */
public class StorageNodeId extends ServiceResourceId
	implements Comparable<StorageNodeId> {

    private static final long serialVersionUID = 1L;
    private static final String SN_PREFIX = "sn";

    private final int storageNodeId;

    public StorageNodeId(int storageNodeId) {
        super();
        this.storageNodeId = storageNodeId;
    }

    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    StorageNodeId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        storageNodeId = in.readInt();
    }

    public static String getPrefix() {
        return SN_PREFIX;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getStorageNodeId
     *      storageNodeId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(storageNodeId);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.STORAGE_NODE;
    }

    public int getStorageNodeId() {
        return storageNodeId;
    }

    @Override
    public int getGroupId() {
        return 0;
    }

    @Override
    public int getNodeNum() {
        return getStorageNodeId();
    }

    /**
     * Returns a string representation that uniquely identifies this SNA.
     *
     * @return the fully qualified name of the SNA
     */
    @Override
    public String getFullName() {
        return SN_PREFIX + storageNodeId;
    }

    /**
     * Parse a string that is either an integer or is in snX format and
     * generate a StorageNodeId.
     */
    public static StorageNodeId parse(String s) {
        return new StorageNodeId(parseForInt(SN_PREFIX, s));
    }

    @Override
    public String toString() {
        return getFullName();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public StorageNode getComponent(Topology topology) {
        return topology.getStorageNodeMap().get(this);
    }

    @Override
    protected StorageNode readComponent(Topology topology,
                                        DataInput in,
                                        short serialVersion)
        throws IOException {

        return new StorageNode(topology, this, in, serialVersion);
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
        StorageNodeId other = (StorageNodeId) obj;
        if (storageNodeId != other.storageNodeId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + storageNodeId;
        return result;
    }

    @Override
    public int compareTo(StorageNodeId other) {
        int x = this.getStorageNodeId();
        int y = other.getStorageNodeId();

        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public StorageNodeId clone() {
        return new StorageNodeId(this.storageNodeId);
    }
}
