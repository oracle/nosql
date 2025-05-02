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
import java.io.IOException;

import oracle.kv.impl.topo.ResourceId.ResourceType;

/**
 * Contains the SN component of the topology.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class StorageNodeMap extends ComponentMap<StorageNodeId, StorageNode> {

    private static final long serialVersionUID = 1L;

    public StorageNodeMap(Topology topology) {
        super(topology);
    }

    StorageNodeMap(Topology topology, DataInput in, short serialVersion)
        throws IOException {

        super(topology, in, serialVersion);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    StorageNodeId nextId() {
        return new StorageNodeId(nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
       return ResourceType.STORAGE_NODE;
    }

    @Override
    Class<StorageNode> getComponentClass() {
        return StorageNode.class;
    }
}
