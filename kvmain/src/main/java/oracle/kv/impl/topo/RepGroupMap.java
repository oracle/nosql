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
 * The RepGroupMap describes the replication groups and the replication nodes
 * underlying the KVStore. It's indexed by the groupId to yield a
 * {@link RepGroup}, which in turn can be indexed by a nodeNum to yield a
 * {@link RepNode}.
 * <p>
 * The map is created and maintained by the GAT as part of the overall Topology
 * associated with the KVStore. Note that both group and ids node nums are
 * assigned from sequences to ensure there is no possibility of inadvertent
 * aliasing across the entire KVStore as groups and nodes are repeatedly
 * created and destroyed.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RepGroupMap extends ComponentMap<RepGroupId, RepGroup> {

    private static final long serialVersionUID = 1L;

    public RepGroupMap(Topology topology) {
        super(topology);
    }

    RepGroupMap(Topology topology, DataInput in, short serialVersion)
        throws IOException {

        super(topology, in, serialVersion);
    }
    
    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#nextId()
     */
    @Override
    RepGroupId nextId() {
       return new RepGroupId(nextSequence());
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.ComponentMap#getResourceType()
     */
    @Override
    ResourceType getResourceType() {
       return ResourceType.REP_GROUP;
    }

    @Override
    Class<RepGroup> getComponentClass() {
        return RepGroup.class;
    }
}
