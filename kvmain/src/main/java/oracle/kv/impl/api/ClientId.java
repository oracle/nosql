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

package oracle.kv.impl.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;

/**
 * A globally unique id, that's used to identify a client making a request to
 * the KV Store.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ClientId extends ResourceId {

    /**
     * The prefix used for cleint names when printed
     */
    private static final String CLIENT_PREFIX = "c";

    private static final long serialVersionUID = 1L;
    /* The unique value. */
    final long clientNum;

    public ClientId(long clientNum) {
        super();
        this.clientNum = clientNum;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    public ClientId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        clientNum = in.readLong();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeLong long}) <i>client num</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeLong(clientNum);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getType()
     */
    @Override
    public ResourceType getType() {
       return ResourceType.CLIENT;
    }

    @Override
    public String toString() {
        return CLIENT_PREFIX + clientNum;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public Component<?> getComponent(Topology topology) {
        throw new UnsupportedOperationException("Method not meaningful for " +
                                                getType());
    }

    @Override
    protected Component<?> readComponent(Topology topology,
                                         DataInput in,
                                         short serialVersion) {

        throw new UnsupportedOperationException(
            "The readComponent method is not supported for ClientId");
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
        ClientId other = (ClientId) obj;
        if (clientNum != other.clientNum) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (clientNum ^ (clientNum >>> 32));
        return result;
    }

    @Override
    public ClientId clone() {
        return new ClientId(this.clientNum);
    }
}
