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
public class DatacenterId extends ResourceId
    implements Comparable<DatacenterId> {

    private static final long serialVersionUID = 1L;

    /** The standard prefix for datacenter IDs. */
    public static final String DATACENTER_PREFIX = "zn";

    /**
     * All acceptable prefixes for datacenter IDs, including the standard one
     * ("zn"), introduced in R3, as well as the previous value ("dc"), for
     * compatibility with earlier releases.
     */
    public static final String[] ALL_DATACENTER_PREFIXES = {"zn", "dc"};

    private final int datacenterId;

    public DatacenterId(int datacenterId) {
        super();
        this.datacenterId = datacenterId;
    }

    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    DatacenterId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        datacenterId = in.readInt();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getDatacenterId
     *      datacenterId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(datacenterId);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.DATACENTER;
    }

    public int getDatacenterId() {
        return datacenterId;
    }

    @Override
    public String toString() {
        return DATACENTER_PREFIX + datacenterId;
    }

    /**
     * Parse a string that is either an integer, or in znX or dcX format, and
     * generate datacenter id.
     */
    public static DatacenterId parse(String s) {
        return new DatacenterId(parseForInt(ALL_DATACENTER_PREFIXES, s));
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent
     *                                          (oracle.kv.impl.topo.Topology)
     */
    @Override
    public Datacenter getComponent(Topology topology) {
        return topology.get(this);
    }

    @Override
    protected Datacenter readComponent(Topology topology,
                                       DataInput in,
                                       short serialVersion)
        throws IOException {

        return Datacenter.readFastExternal(topology, this, in, serialVersion);
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

        final DatacenterId other = (DatacenterId) obj;
        if (datacenterId != other.datacenterId) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + datacenterId;
        return result;
    }

    @Override
    public int compareTo(DatacenterId other) {
        return this.datacenterId - other.datacenterId;
    }

    @Override
    public DatacenterId clone() {
        return new DatacenterId(this.datacenterId);
    }
}
