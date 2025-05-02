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
 * Defines the RepGroup internal map used collect its rep nodes
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RepNodeComponentMap extends ComponentMap<RepNodeId, RepNode> {

    private static final long serialVersionUID = 1L;

    /* The owning rep group. */
    private final RepGroup repGroup;

    public RepNodeComponentMap(RepGroup repGroup, Topology topology) {
        super(topology);
        this.repGroup = repGroup;
    }

    RepNodeComponentMap(RepGroup repGroup,
                        Topology topology,
                        DataInput in,
                        short serialVersion)
        throws IOException {

        super(topology, in, serialVersion);
        this.repGroup = repGroup;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    RepNodeId nextId() {
        return  new RepNodeId(this.repGroup.getResourceId().getGroupId(),
                              nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
       return ResourceType.REP_NODE;
    }

    @Override
    Class<RepNode> getComponentClass() {
        return RepNode.class;
    }
}
