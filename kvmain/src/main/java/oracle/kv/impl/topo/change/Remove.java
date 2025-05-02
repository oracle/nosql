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

package oracle.kv.impl.topo.change;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology.Component;

/**
 * Removes an existing component from the Topology
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Remove extends TopologyChange {

    private static final long serialVersionUID = 1L;
    private final ResourceId resourceId;

    /**
     * @param sequenceNumber
     */
    public Remove(int sequenceNumber,
                  ResourceId resourceId) {
        super(sequenceNumber);

        checkNull("resourceId", resourceId);
        this.resourceId = resourceId;
    }

    Remove(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        resourceId = ResourceId.readFastExternal(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link TopologyChange}) {@code super}
     * <li> ({@link ResourceId}) {@link #getResourceId resourceId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        resourceId.writeFastExternal(out, serialVersion);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.change.TopologyChange#getType()
     */
    @Override
    public Type getType() {
        return Type.REMOVE;
    }

    @Override
    public ResourceId getResourceId() {
        return resourceId;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.change.TopologyChange#getComponent()
     */
    @Override
    public Component<?> getComponent() {
        return null;
    }

    @Override
    public Remove clone() {
        return new Remove(sequenceNumber, resourceId);
    }
}
