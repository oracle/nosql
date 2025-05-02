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
import java.io.ObjectInputStream;
import java.util.Collection;

import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.ObjectNode;

/**
 * The RepGroup in a {@link RepGroupMap}. It identifies the RNs within the
 * group.
 * <p>
 * Note that a RepGroup simply serves to group RepNodes. It does not have any
 * attributes associated with it.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class RepGroup extends Component<RepGroupId> {

    private static final long serialVersionUID = 1L;
    private final ComponentMap<RepNodeId, RepNode> repNodeMap;
    private ComponentMap<ArbNodeId, ArbNode> arbNodeMap;


    public RepGroup() {
        repNodeMap = new RepNodeComponentMap(this, null);
        arbNodeMap = new ArbNodeComponentMap(this, null);
    }

    /**
     * Note that this constructor does not copy the component map. It's
     * intended exclusively for use by the cloning operation below to create a
     * RepGroup entry in the topology change list.
     */
    private RepGroup(RepGroup repGroup) {
        super(repGroup);
        repNodeMap = new RepNodeComponentMap(this, null);
        arbNodeMap = new ArbNodeComponentMap(this, null);
    }

    RepGroup(Topology topology,
             RepGroupId rgId,
             DataInput in,
             short serialVersion)
        throws IOException {

        super(topology, rgId, in, serialVersion);
        repNodeMap =
            new RepNodeComponentMap(this, topology, in, serialVersion);
        arbNodeMap =
            new ArbNodeComponentMap(this, topology, in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Component}) {@code super}
     * <li> ({@link RepNodeComponentMap}) <i>RepNode map</i>
     * <li> ({@link ArbNodeComponentMap}) <i>ArbNode map</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        repNodeMap.writeFastExternal(out, serialVersion);
        arbNodeMap.writeFastExternal(out, serialVersion);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#getResourceType()
     */
    @Override
    public ResourceType getResourceType() {
        return ResourceType.REP_GROUP;
    }

    public Collection<RepNode> getRepNodes() {
        return repNodeMap.getAll();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#clone()
     */
    @Override
    public Component<?> clone() {
        return new RepGroup(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result +
            ((repNodeMap == null) ? 0 : repNodeMap.hashCode());
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
        RepGroup other = (RepGroup) obj;
        if (repNodeMap == null) {
            if (other.repNodeMap != null) {
                return false;
            }
        } else if (!repNodeMap.equals(other.repNodeMap)) {
            return false;
        } else if (!super.equals(obj)) {
                return false;
        }
        return true;
    }

    public RepNode get(RepNodeId repNodeId) {
       return repNodeMap.get(repNodeId);
    }

    public RepNode add(RepNode repNode) {
        return repNodeMap.add(repNode);
    }

    public RepNode update(RepNodeId resourceId,
                          RepNode repNode) {
        return repNodeMap.update(resourceId, repNode);
    }

    public RepNode remove(RepNodeId repNodeId) {
        return repNodeMap.remove(repNodeId);
    }

    public void apply(TopologyChange change) {
        repNodeMap.apply(change);
    }

    public Collection<ArbNode> getArbNodes() {
        return arbNodeMap.getAll();
    }

    public ArbNode get(ArbNodeId arbNodeId) {
        return arbNodeMap.get(arbNodeId);
    }

    public ArbNode add(ArbNode arbNode) {
        return arbNodeMap.add(arbNode);
    }

    public ArbNode update(ArbNodeId resourceId,
                          ArbNode arbNode) {
        return arbNodeMap.update(resourceId, arbNode);
    }

    public ArbNode remove(ArbNodeId arbNodeId) {
        return arbNodeMap.remove(arbNodeId);
    }

    public void applyArbChange(TopologyChange change) {
        arbNodeMap.apply(change);
    }

    /**
     * Wraps the set method to ensure that the "topology" associated with the
     * repNodeMap is set appropriately as well.
     */
    @Override
    public void setTopology(Topology topology) {
        super.setTopology(topology);
        repNodeMap.setTopology(topology);
        arbNodeMap.setTopology(topology);
    }

    @Override
    public String toString() {
        return "[" + getResourceId() + "]";
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put("resourceId", getResourceId().toString());
        return top;
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (arbNodeMap == null) {
            arbNodeMap = new ArbNodeComponentMap(this, null);
        }
    }
}
