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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;

/**
 * Abstract implementation of privileges within KVStore security system. For
 * each KVStore privilege label (see {@link KVStorePrivilegeLabel}), we can
 * create an instance describing its detailed information, which includes the
 * implied privileges, specific resource subject to access control and so
 * forth.
 */
public abstract class KVStorePrivilege
        implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Four categories of privileges in KVStore: system privilege, object
     * privilege, namespace privilege and table privilege. System privileges
     * apply to all operations of a certain sort, while object privileges are
     * limited to particular objects. Especially, table privileges are
     * applicable for tables only.
     * <p>
     * From R3.2, the OBJECT type becomes obsolete, and will be replace by more
     * specific object types like TABLE.
     */
    public static enum PrivilegeType { SYSTEM, OBJECT, TABLE, NAMESPACE }

    /* The privilege label associated with this privilege instance. */
    private final KVStorePrivilegeLabel privLabel;

    /*
     * TODO:
     * Keep associated resource information for object privileges in future.
     */

    /**
     * Constructs a privilege instance using the specified label.
     *
     * @param privLabel KVStore privilege label
     */
    KVStorePrivilege(KVStorePrivilegeLabel privLabel) {
        this.privLabel = privLabel;
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
    }

    /**
     * Checks for equality.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return privLabel.equals(
            ((KVStorePrivilege) other).privLabel);
    }

    /**
     * Gets the hashCode value for the object
     */
    @Override
    public int hashCode() {
        return privLabel.hashCode();
    }

    /**
     * Gets a string representation of this privilege
     */
    @Override
    public String toString() {
        return privLabel.toString();
    }

    /**
     * Returns the label associated with this privilege
     */
    public KVStorePrivilegeLabel getLabel() {
        return privLabel;
    }

    /**
     * Returns the type of this privilege
     */
    public PrivilegeType getType() {
        return privLabel.getType();
    }

    /**
     * Returns the privileges "implying" this privilege.  This must be
     * implemented by subclasses of privilege, as they are the only ones that
     * can impose semantics on a privilege object.
     *
     * @return an array of privileges implying this privilege. If no privilege
     * implies this one, an empty array will be returned.
     */
    public abstract KVStorePrivilege[] implyingPrivileges();

    /** Reads a KVStorePrivilege subclass instance. */
    public static KVStorePrivilege readKVStorePrivilege(DataInput in,
                                                        short serialVersion)
        throws IOException
    {
        return KVStorePrivilegeLabel.readFastExternal(in, serialVersion)
            .readPrivilege(in, serialVersion);
    }

    /** Writes this KVStorePrivilege subclass instance. */
    public void writeKVStorePrivilege(DataOutput out, short serialVersion)
        throws IOException
    {
        getLabel().writeFastExternal(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }
}
