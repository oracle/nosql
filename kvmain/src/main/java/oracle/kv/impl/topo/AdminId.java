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
 * A unique identifier used by each of the instances of the Admin Service.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class AdminId extends ServiceResourceId implements Comparable<AdminId> {

    private static final long serialVersionUID = 1;
    private static final String ADMIN_PREFIX = "admin";

    /**
     * The unique ID of this admin instance.
     */
    private final int nodeId;

    /**
     * Creates an ID of an admin instance.
     *
     * @param nodeId the internal node id
     */
    public AdminId(int nodeId) {
        super();
        this.nodeId = nodeId;
    }

    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    AdminId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        nodeId = in.readInt();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getAdminInstanceId
     *      nodeId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(nodeId);
    }

    /**
     * Gets the type of this resource
     *
     * @return the resource type
     */
    @Override
    public ResourceType getType() {
        return ResourceId.ResourceType.ADMIN;
    }

    /**
     * Gets the internal identifier of this admin instance
     *
     * @return the instance ID
     */
    public int getAdminInstanceId() {
        return nodeId;
    }

    @Override
    public int getNodeNum() {
        return getAdminInstanceId();
    }

    @Override
    public int getGroupId() {
        return 0;
    }

    @Override
    public String getFullName() {
        return ADMIN_PREFIX + getAdminInstanceId();
    }

    /**
     * Parse a string that is either an integer or is in adminX format and
     * generate an AdminId
     */
    public static AdminId parse(String s) {
        return new AdminId(parseForInt(ADMIN_PREFIX, s));
    }

    /**
     * Return the admin prefix string.
     */
    public static String getPrefix() {
        return ADMIN_PREFIX;
    }

    @Override
    public String toString() {
        return getFullName();
    }

    @Override
    public Topology.Component<?> getComponent(Topology topology) {
        /*
         * AdminId does not correspond to a real Topology component, so
         * we can't satisfy this request.
         */
        throw new UnsupportedOperationException
            ("Method not implemented: getComponent");
    }

    @Override
    protected Topology.Component<?> readComponent(Topology topology,
                                                  DataInput in,
                                                  short serialVersion) {

        throw new UnsupportedOperationException(
            "The readComponent method not supported for AdminId");
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
        AdminId other = (AdminId) obj;
        if (nodeId != other.nodeId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + nodeId;
        return result;
    }

    @Override
    public int compareTo(AdminId other) {
        int x = this.getAdminInstanceId();
        int y = other.getAdminInstanceId();

        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public AdminId clone() {
        return new AdminId(this.nodeId);
    }
}
