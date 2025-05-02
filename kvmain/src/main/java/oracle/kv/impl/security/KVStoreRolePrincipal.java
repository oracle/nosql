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

import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.security.Principal;

import javax.security.auth.Subject;

/**
 * Represents a "Role" associated with user.  This exists so that the roles
 * can be attached to subjects as a "Principal".
 */
public final class KVStoreRolePrincipal
        implements KVStorePrincipal, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Reserved old role field to maintain compatibility during upgrade, only
     * accept the value of KVStoreRoles.
     */
    private final KVStoreRole role;

    /**
     * The name of KVStore role assigned to an identity.
     */
    private String roleName;

    /**
     * The role associated with the public users.
     */
    public static final KVStoreRolePrincipal PUBLIC =
        new KVStoreRolePrincipal(RoleInstance.PUBLIC_NAME);

    /**
     * The role associated with the database administrators.
     */
    public static final KVStoreRolePrincipal DBADMIN =
        new KVStoreRolePrincipal(RoleInstance.DBADMIN_NAME);

    /**
     * The role associated with the store administrators.
     */
    public static final KVStoreRolePrincipal SYSADMIN =
        new KVStoreRolePrincipal(RoleInstance.SYSADMIN_NAME);

    /**
     * The role associated with the read-only users.
     */
    public static final KVStoreRolePrincipal READONLY =
        new KVStoreRolePrincipal(RoleInstance.READONLY_NAME);

    /**
     * The role associated with the write-only users.
     */
    public static final KVStoreRolePrincipal WRITEONLY =
        new KVStoreRolePrincipal(RoleInstance.WRITEONLY_NAME);

    /**
     * The role associated with the read-write users.
     */
    public static final KVStoreRolePrincipal READWRITE =
        new KVStoreRolePrincipal(RoleInstance.READWRITE_NAME);

    /**
     * The role associated with the server itself.
     */
    public static final KVStoreRolePrincipal INTERNAL =
        new KVStoreRolePrincipal(KVStoreRole.INTERNAL);

    /*
     * Reserved old R3 KVStore role principles
     */

    /**
     * The R3 role associated with admin users.
     */
    public static final KVStoreRolePrincipal ADMIN =
        new KVStoreRolePrincipal(KVStoreRole.ADMIN);

    /**
     * The R3 role associated with logged-in users (or internal).
     */
    public static final KVStoreRolePrincipal AUTHENTICATED =
        new KVStoreRolePrincipal(KVStoreRole.AUTHENTICATED);

    /**
     * A map to allow mapping from role name to the corresponding canonical
     * KVStoreRolePrincipal.
     */
    private static final Map<String, KVStoreRolePrincipal> roleMap =
        new HashMap<String, KVStoreRolePrincipal>();

    static {
        roleMap.put(INTERNAL.getName(), INTERNAL);
        roleMap.put(PUBLIC.getName(), PUBLIC);
        roleMap.put(DBADMIN.getName(), DBADMIN);
        roleMap.put(SYSADMIN.getName(), SYSADMIN);
        roleMap.put(READONLY.getName(), READONLY);
        roleMap.put(WRITEONLY.getName(), WRITEONLY);
        roleMap.put(READWRITE.getName(), READWRITE);
        roleMap.put(ADMIN.getName(), ADMIN);
        roleMap.put(AUTHENTICATED.getName(), AUTHENTICATED);
    }

    /**
     * Find the KVStoreRolePrincipal instance corresponding to a role name.
     * The role name will first be resolved as a built-in role and then, if
     * fails, as a user-defined role
     */
    public static KVStoreRolePrincipal get(final String roleName) {
        final KVStoreRolePrincipal rolePrinc =
            roleMap.get(RoleInstance.getNormalizedName(roleName));
        return rolePrinc == null ?
               new KVStoreRolePrincipal(roleName) :
               rolePrinc;
    }

    /**
     * Return an array of names of all KVStoreRoles associated with a Subject.
     * @param subj the subject to consider
     * @return a newly allocated array containing the names assigned roles
     */
    public static String[] getSubjectRoleNames(Subject subj) {
        return ExecutionContext.getSubjectRoles(subj);
    }

    /*
     * Constructors
     */

    /**
     * Constructor used for old R3 role principals with normalized role name.
     */
    private KVStoreRolePrincipal(KVStoreRole role) {
        this.role = role;
        this.roleName = RoleInstance.getNormalizedName(role.toString());
    }

    private KVStoreRolePrincipal(String roleName) {
        if (roleName == null) {
            throw new IllegalArgumentException("Role name should not be null");
        }
        this.role = null;
        this.roleName = RoleInstance.getNormalizedName(roleName);
    }

    /**
     * Creating a R3 role principal with R3 role name for backward
     * compatibility.
     */
    private KVStoreRolePrincipal(KVStoreRole role, String roleName) {
        this.role = role;
        this.roleName = roleName;
    }

    KVStoreRolePrincipal(DataInput in, short serialVersion)
        throws IOException
    {
        role = readFastExternalOrNull(in, serialVersion,
                                      KVStoreRole::readFastExternal);
        roleName = readString(in, serialVersion);
    }

    /*
     * FastExternalizable interface methods
     */

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writeFastExternalOrNull(out, serialVersion, role);
        writeString(out, serialVersion, roleName);
    }

    /*
     * KVStorePrincipal interface methods
     */

    @Override
    public PrincipalType getPrincipalType() {
        return PrincipalType.ROLE;
    }

    /*
     * Principal interface methods
     */

    /**
     * Check for equality.
     * @see Object#equals
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return roleName.equals(((KVStoreRolePrincipal) other).roleName);
    }

    /**
     * Get the principal name.
     * @see Principal#getName
     */
    @Override
    public String getName() {
        return roleName;
    }

    /**
     * Get the hashCode value for the object.
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return roleName.hashCode();
    }

    /**
     * Get a string representation of this object.
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return "KVStoreRolePrincipal(" + roleName + ")";
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();

        /* Used to read the object created in R3 code */
        if (role != null) {
            if (roleName == null) {

                /*
                 * Role name normalization is performed by role resolver.
                 * TODO: Move all normalization here in future.
                 */
                roleName = role.toString();
            } else if (!role.toString().equalsIgnoreCase(roleName)) {
                throw new IOException(
                    String.format("Role %s and role name %s are mismatched",
                                  role, roleName));
            }
        } else {
            if (roleName == null) {
                throw new IOException(
                    "Unexpected error when read KVStoreRolePrinciple object," +
                    "role and roleName are both null");
            }
        }
    }

    /**
     * Replace R3 role principals with a R3-compatible object before the
     * serialization.
     */
    private Object writeReplace() {

        if (this.role != null) {
            return new KVStoreRolePrincipal(this.role, this.role.toString());
        }
        return this;
    }
}
