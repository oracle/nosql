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

import static oracle.kv.impl.security.KVStorePrivilege.PrivilegeType.NAMESPACE;
import static oracle.kv.impl.security.KVStorePrivilege.PrivilegeType.SYSTEM;
import static oracle.kv.impl.security.KVStorePrivilege.PrivilegeType.TABLE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.security.KVStorePrivilege.PrivilegeType;
import oracle.kv.impl.util.FastExternalizable;

/**
 * A set of labels denoting system-defined privileges within the KVStore
 * security system. The privilege labels are mainly used in annotations of RMI
 * to denote the required privileges.
 */
public enum KVStorePrivilegeLabel implements FastExternalizable {

    /*
     * Privileges for data access
     */

    /**
     * Get/iterate keys and values in entire store including any tables
     */
    READ_ANY(0, SYSTEM),

    /**
     * Put/delete values in entire store including any tables
     */
    WRITE_ANY(1, SYSTEM),

    /**
     * Privileges for administrative tasks
     */

    /**
     * Privilege to perform ONDB database management
     */
    SYSDBA(2, SYSTEM),

    /**
     * Privilege to view/show system information, configuration and metadata
     */
    SYSVIEW(3, SYSTEM),

    /**
     * Privilege to query data object information
     */
    DBVIEW(4, SYSTEM),

    /**
     * Privilege to query users' own information
     */
    USRVIEW(5, SYSTEM),

    /**
     * Privilege to perform ONDB administrative tasks
     */
    SYSOPER(6, SYSTEM),

    /**
     * Privilege for KVStore component to access internal services and internal
     * keyspace.
     */
    INTLOPER(7, SYSTEM),

    /**
     * Privileges for reading Avro schemas.
     */
    READ_ANY_SCHEMA(8, SYSTEM),

    /**
     * Privileges for adding, removing and updating Avro schemas
     */
    WRITE_ANY_SCHEMA(9, SYSTEM),

    /**
     * Privileges for creating any table in kvstore
     */
    CREATE_ANY_TABLE(10, SYSTEM),

    /**
     * Privileges for dropping any table in kvstore
     */
    DROP_ANY_TABLE(11, SYSTEM),

    /**
     * Privileges for evolving any table in kvstore
     */
    EVOLVE_ANY_TABLE(12, SYSTEM),

    /**
     * Privileges for creating index on any table in kvstore
     */
    CREATE_ANY_INDEX(13, SYSTEM),

    /**
     * Privileges for dropping index on any table in kvstore
     */
    DROP_ANY_INDEX(14, SYSTEM),

    /**
     * Privileges for reading data in any table in kvstore
     */
    READ_ANY_TABLE(15, SYSTEM),

    /**
     * Privileges for deleting data in any table in kvstore
     */
    DELETE_ANY_TABLE(16, SYSTEM),

    /**
     * Privileges for inserting data in any table in kvstore
     */
    INSERT_ANY_TABLE(17, SYSTEM),

    /**
     * Privilege for getting/iterating key-values from a specific table
     */
    READ_TABLE(18, TABLE),

    /**
     * Privilege for deleting key-values in a specific table
     */
    DELETE_TABLE(19, TABLE),

    /**
     * Privilege for putting key-values in a specific table
     */
    INSERT_TABLE(20, TABLE),

    /**
     * Privilege for evolving a specific table
     */
    EVOLVE_TABLE(21, TABLE),

    /**
     * Privilege for creating index on a specific table
     */
    CREATE_INDEX(22, TABLE),

    /**
     * Privilege for dropping index on a specific table
     */
    DROP_INDEX(23, TABLE),

    /**
     * Privileges for creating a new namespace in kvstore.
     */
    CREATE_ANY_NAMESPACE(24, SYSTEM),

    /**
     * Privileges for dropping any namespace in kvstore.
     */
    DROP_ANY_NAMESPACE(25, SYSTEM),

    /**
     * Privileges for creating tables inside a specific namespace.
     */
    CREATE_TABLE_IN_NAMESPACE(26, NAMESPACE),

    /**
     * Privileges for dropping tables inside a specific namespace.
     */
    DROP_TABLE_IN_NAMESPACE(27, NAMESPACE),

    /**
     * Privileges for evolving tables inside a specific namespace.
     */
    EVOLVE_TABLE_IN_NAMESPACE(28, NAMESPACE),

    /**
     * Privileges for creating indexes inside a specific namespace.
     */
    CREATE_INDEX_IN_NAMESPACE(29, NAMESPACE),

    /**
     * Privileges for dropping indexes inside a specific namespace.
     */
    DROP_INDEX_IN_NAMESPACE(30, NAMESPACE),

    /**
     * Helper label only for CREATE_TABLE_IN_NAMESPACE, DROP_TABLE_IN_NAMESPACE,
     * EVOLVE_TABLE_IN_NAMESPACE, CREATE_INDEX_IN_NAMESPACE and
     * DROP_INDEX_IN_NAMESPACE privileges.
     */
    MODIFY_IN_NAMESPACE(31, NAMESPACE),

    /**
     * Privilege for getting/iterating items within tables from a specific
     * namespace.
     */
    READ_IN_NAMESPACE(32, NAMESPACE),

    /**
     * Privilege for creating items within tables in a specific namespace.
     */
    INSERT_IN_NAMESPACE(33, NAMESPACE),

    /**
     * Privilege for deleting items within tables in a specific namespace.
     */
    DELETE_IN_NAMESPACE(34, NAMESPACE),

    /**
     * Privileges for creating a new region.
     */
    CREATE_ANY_REGION(35, SYSTEM),

    /**
     * Privileges for dropping a region.
     */
    DROP_ANY_REGION(36, SYSTEM),

    /**
     * An additional privilege that is needed in order to write any system
     * table.
     */
    WRITE_SYSTEM_TABLE(37, SYSTEM),

    /**
     * Privileges for setting the local region name.
     */
    SET_LOCAL_REGION(38, SYSTEM);

    private static final KVStorePrivilegeLabel[] VALUES = values();
    private final PrivilegeType type;

    KVStorePrivilegeLabel(final int ordinal,
                          final PrivilegeType type) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
        this.type = type;
    }

    static KVStorePrivilegeLabel readFastExternal(DataInput in,
                                                  @SuppressWarnings("unused")
                                                  short sv)
        throws IOException
    {
        final int ordinal = in.readByte();
        try {
            return VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                "Unknown KVStorePrivilegeLabel: " + ordinal);
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeByte(ordinal());
    }

    public PrivilegeType getType() {
        return type;
    }

    /** Reads a KVStorePrivilege subclass. */
    KVStorePrivilege readPrivilege(DataInput in, short sv) throws IOException {
        switch (type) {
        case SYSTEM:
            return SystemPrivilege.get(this);
        case TABLE:
            return TablePrivilege.readTablePrivilege(in, sv, this);
        case NAMESPACE:
            return NamespacePrivilege.readNamespacePrivilege(in, sv, this);
        default:
            throw new IOException("Unexpected privilege type: " + type);
        }
    }
}
