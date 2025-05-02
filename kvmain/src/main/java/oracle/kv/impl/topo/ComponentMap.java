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

import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullCollection;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * The Map implementation underlying the map components that make up the
 * Topology
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract class ComponentMap<RID extends ResourceId,
                            COMP extends Topology.Component<RID>>
    implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1L;

    protected Topology topology;

    /* Components stored in this map are assigned sequence number from here */
    private int idSequence = 0;

    protected final HashMap<RID, COMP> cmap = new HashMap<>();

    public ComponentMap(Topology topology) {
        super();
        this.topology = topology;
    }

    protected ComponentMap(Topology topology,
                           DataInput in,
                           short serialVersion)
        throws IOException {

        this.topology = topology;
        idSequence = readPackedInt(in);
        final int size = readNonNullSequenceLength(in);
        for (int i = 0; i < size; i++) {
            put(readComponent(in, serialVersion));
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packedInt}) {@link
     *      #getLastSequence idSequence}
     * <li> ({@link SerializationUtil#writeNonNullCollection non-null
     *      collection}) {@link #getAll components}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writePackedInt(out, idSequence);
        writeNonNullCollection(out, serialVersion, cmap.values());
    }

    /**
     * Read a component map element from the input stream
     *
     * @param in the input stream
     * @param serialVersion the version of the serialization format
     * @return the component
     * @throws IOException if there is a problem reading from the input stream
     */
    COMP readComponent(DataInput in, short serialVersion)
        throws IOException {

        final ResourceId rId = ResourceId.readFastExternal(in, serialVersion);
        final Topology.Component<?> component =
            rId.readComponent(topology, in, serialVersion);
        final Class<COMP> componentClass = getComponentClass();
        if (!componentClass.isInstance(component)) {
            throw new IOException(
                "Expected " + componentClass.getSimpleName() + ": " +
                component);
        }
        return componentClass.cast(component);
    }

    public void setTopology(Topology topology) {
       this.topology = topology;
    }

    /**
     * Returns the next ID in sequence. The sequence is increased by 1.
     *
     * @return the next ID
     */
    public int nextSequence() {
        return ++idSequence;
    }

    /**
     * Returns the last ID returned by nextSequence.
     *
     * @return the last ID returned by nextSequence
     */
    public int getLastSequence() {
        return idSequence;
    }

    /**
     * Returns the component associated with the resourceId
     *
     * @param resourceId the id used for the lookup
     *
     * @return the component if it exists
     */
    public COMP get(RID resourceId) {
        return cmap.get(resourceId);
    }

    public COMP put(COMP component) {
        return cmap.put(component.getResourceId(), component);
    }

    /**
     * Remove all components from this map and reset the id sequence, in
     * order to create a "blank" map for comparison purposes.
     */
    public void reset() {
        idSequence = 0;
        cmap.clear();
    }

    /*
     * Returns the next id that served as the basis for a new component
     * in the Component type
     */
    abstract RID nextId();

    /* Returns the type of resource id index used by this map. */
    abstract ResourceId.ResourceType getResourceType();

    /**
     * Returns the subclass of {@link Topology.Component} for components of
     * this map.
     */
    abstract Class<COMP> getComponentClass();

    /**
     * Creates a new Topology component and adds it to the map assigning it
     * with a newly created unique resourceId.
     *
     * @param component to be added
     *
     * @return the component modified with a resource id, that is now part of
     * the topology
     */
    COMP add(COMP component) {
        return add(component, nextId());
    }

    /**
     * Create a new topology component with a preset resource id (for
     * partitions).
     *
     * TODO: confer with Sam.
     */
    COMP add(COMP component, RID resId) {
        if (component.getTopology() != null) {
            throw new IllegalArgumentException("component is already a part " +
                                                "of a Topology");
        }
        component.setTopology(topology);
        component.setResourceId(resId);
        final COMP prev = put(component);
        topology.getChangeTracker().logAdd(component);
        if (prev != null) {
            throw new IllegalStateException(prev +
                                            " was overwritten in topology by " +
                                            component);
        }
        return component;
    }

    /**
     * Updates the component associated with the resource id
     *
     * @param component to be added
     *
     * @return the component modified with a resource id, that is now part of
     * the topology
     */
    COMP update(RID resourceId, COMP component) {
        return update(resourceId, component, Topology.NULL_LOCALIZATION);
    }

    /**
     * Updates the component associated with the resource id
     *
     * @param component to be added
     * @param localizationNumber the localization number if the change is
     * localized
     *
     * @return the component modified with a resource id, that is now part of
     * the topology
     */
    COMP update(RID resourceId, COMP component, long localizationNumber) {
        if ((component.getTopology() != null) ||
            (component.getResourceId() != null)) {
            throw new IllegalArgumentException("component is already a part " +
                                                "of a Topology");
        }

        @SuppressWarnings("unchecked")
        COMP prev = (COMP)topology.get(resourceId);
        if (prev == null) {
            throw new IllegalArgumentException("component: " + resourceId +
                                               " absent from Topology");
        }
        /* Clear just the topology, retain the resource id. */
        prev.setTopology(null);
        component.setTopology(topology);
        component.setResourceId(resourceId);
        prev = put(component);
        assert prev != null;
        topology.getChangeTracker().logUpdate(component, localizationNumber);
        return component;
    }

    /**
     * Removes the component associated with the resourceId
     *
     * @param resourceId the id used for the lookup
     *
     * @return the previous component associated with the ResourceId
     */
    public COMP remove(RID resourceId) {
        COMP component = cmap.remove(resourceId);
        if (component == null) {
            throw new IllegalArgumentException("component: " + resourceId +
                                               " absent from Topology");
        }
        /* Clear just the topology, retain the resource id. */
        component.setTopology(null);
        topology.getChangeTracker().logRemove(resourceId);
        return component;
    }

    public int size() {
        return cmap.size();
    }

    /**
     * Returns true if this map is empty.
     *
     * @return true if this map is empty
     */
    public boolean isEmpty() {
        return cmap.isEmpty();
    }

    /**
     * Returns all the components present in the map
     */
    public Collection<COMP> getAll() {
        return cmap.values();
    }

    /**
     * Return the resource ids for all components present in the map.
     */
    public Set<RID> getAllIds() {
        return cmap.keySet();
    }

    /**
     * Applies the change to the map. The various sequences need to be dealt
     * with carefully.
     * <p>
     * If the operation is an Add, the generated resourceId must match the
     * one that was passed in.
     * <p>
     * The change sequence numbers must also align up currently.
     */
    public void apply(TopologyChange change) {
         final ResourceId resourceId = change.getResourceId();

         if (change.getType() == TopologyChange.Type.REMOVE) {
            COMP prev = cmap.remove(resourceId);
            if (prev == null) {
                throw new IllegalStateException
                    ("Delete operation detected missing component: " +
                     resourceId);
            }
            topology.getChangeTracker().logRemove(resourceId);

            return;
         }

         @SuppressWarnings("unchecked")
         COMP prev = (COMP)topology.get(resourceId);

         @SuppressWarnings("unchecked")
         COMP curr = (COMP)change.getComponent();
         curr.setTopology(topology);
         int logSeqNumber = curr.getSequenceNumber();

         prev = put(curr);

         switch (change.getType()) {

             case ADD:
                 /* Consume a resource Id */
                 RID consumeId = nextId();
                 if (!consumeId.equals(resourceId)) {
                     throw new IllegalStateException
                         ("resource sequence out of sync; expected: " +
                          consumeId + " replayId: " + resourceId);
                 }
                 if (prev != null) {
                     throw new IllegalStateException
                         ("ADD operation found existing component: " +
                          resourceId);
                 }
                 topology.getChangeTracker().logAdd(curr);
                 break;

             case UPDATE:
                 if (prev == null) {
                     throw new IllegalStateException
                         ("UPDATE operation detected missing component" +
                          resourceId);
                 }
                 /* Clear just the topology, retain the resource id. */
                 prev.setTopology(null);
                 topology.getChangeTracker().logUpdate(curr);
                 break;

             default:
                 throw new IllegalStateException
                     ("unexpected type: " + change.getType());
         }

         if (curr.getSequenceNumber() != logSeqNumber) {
             throw new IllegalStateException
                 ("change sequence mismatch; log #: " + logSeqNumber +
                  " replay #: " + curr.getSequenceNumber());
         }
     }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cmap == null) ? 0 : cmap.hashCode());
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

        ComponentMap<?, ?> other = (ComponentMap<?, ?>) obj;
        if (cmap == null) {
            if (other.cmap != null) {
                return false;
            }
        } else if (!cmap.equals(other.cmap)) {
            return false;
        }

        return true;
    }

    /**
     * Returns a characteristic byte array describing the component information
     * of a topology, for use in signature generation.  The returned value
     * needs to reflect all the child components of a topology, but should be
     * independent of the containing topology. To ensure returning the same
     * result of a same topology each time, all child components need to be
     * sorted by resources IDs, since they are stored in Hash containers.
     *
     * @throws IOException if any problem happens while generating the byte
     * arrays
     */
    public byte[] toByteArrayForSignature() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        try {
            oos.writeByte(idSequence);

            if (cmap != null && !cmap.isEmpty()) {
                final SortedMap<RID, COMP> sortedCmap =
                    new TreeMap<RID, COMP>(cmap);
                for (COMP comp : sortedCmap.values()) {
                    oos.writeObject(comp.cloneForLog());
                }
            }
        } finally {
            try {
                oos.close();
            } catch (IOException ioe) {
                /* Ignore */
            }
        }

        return baos.toByteArray();
    }

    @Override
    public String toString() {
        return cmap.toString();
    }
}
