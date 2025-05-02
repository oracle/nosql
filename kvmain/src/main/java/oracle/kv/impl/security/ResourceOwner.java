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

package oracle.kv.impl.security;

import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ObjectUtil;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */

/**
 * A simple structure recording the owner of an resource in KVStore security
 * systems, including plan, table, and keyspace in future. General, an owner of
 * a resource is a KVStoreUser. Here only the id and the user name are recorded
 * for simplicity.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ResourceOwner implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final String name;

    public ResourceOwner(String id, String name) {
        ObjectUtil.checkNull("id", id);
        ObjectUtil.checkNull("name", name);
        this.id = id;
        this.name = name;
    }

    /* Copy ctor */
    public ResourceOwner(ResourceOwner other) {
        ObjectUtil.checkNull("Other owner", other);
        this.id = other.id;
        this.name = other.name;
    }

    /**
     * Constructor for FastExternalizable.
     */
    public ResourceOwner(DataInput in, short serialVersion) throws IOException {
        id = readNonNullString(in, serialVersion);
        name = readNonNullString(in, serialVersion);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString
     *      non-null String}) {@code id}
     * <li> ({@link SerializationUtil#writeNonNullString
     *      non-null String}) {@code name}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        writeNonNullString(out, serialVersion, id);
        writeNonNullString(out, serialVersion, name);
    }

    /**
     * constructs ResourceOwner from a string created by toString().
     */
    public static ResourceOwner fromString(String resourceString) {
        int lp = resourceString.indexOf('(');
        int rp = resourceString.indexOf(')');
        String nameString = resourceString.substring(0, lp);
        String idString = resourceString.substring((lp+4), rp);
        return new ResourceOwner(idString, nameString);
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String toJsonString() {
        return "{\"name\":\"" + name +
                "\",\"id\":\"" + id + "\"}";
    }

    @Override
    public String toString() {
        return String.format("%s(id:%s)", name, id);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ResourceOwner)) {
            return false;
        }
        final ResourceOwner otherOwner = (ResourceOwner) other;
        return id.equals(otherOwner.id) && name.equals(otherOwner.name);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + id.hashCode();
        result = result * prime + name.hashCode();
        return result;
    }
}
