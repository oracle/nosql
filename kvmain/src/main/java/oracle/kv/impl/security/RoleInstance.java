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

import static java.util.Locale.ENGLISH;
import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;

/**
 * Representation of roles within the KVStore security system.
 */
public class RoleInstance extends SecurityMetadata.SecurityElement {

    private static final long serialVersionUID = 1L;

    /**
     * Case-sensitive name of the role.
     */
    private final String name;

    /**
     * A set containing all granted privileges of the role.
     */
    private final Set<KVStorePrivilege> grantedPrivileges;

    /**
     * Indicates whether the role can be explicitly granted to/revoked from a
     * user by the grant/revoke commands.
     */
    private final boolean assignable;

    /**
     * Indicates whether the information role contains can be modified, like
     * grant new roles or privileges to this role.
     */
    private final boolean readonly;

    /**
     * A set containing all granted roles of this role.
     */
    private final Set<String> grantedRoles;

    /* Names for system predefined roles */
    public static final String READONLY_NAME = "readonly";
    public static final String WRITEONLY_NAME = "writeonly";
    public static final String READWRITE_NAME = "readwrite";
    public static final String DBADMIN_NAME = "dbadmin";
    public static final String SYSADMIN_NAME = "sysadmin";
    public static final String PUBLIC_NAME = "public";
    public static final String WRITESYSTABLE_NAME = "writesystable";

    /**
     * Users capable of reading data from store only.
     */
    public static final RoleInstance READONLY =
        new RoleInstance(READONLY_NAME,
                        new KVStorePrivilege[] { SystemPrivilege.READ_ANY },
                        true /* assignable */,
                        new String[0] /* grantedRoles */);

    /**
     * Users capable to writing data into store only.
     */
    public static final RoleInstance WRITEONLY =
        new RoleInstance(WRITEONLY_NAME,
                        new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY },
                        true /* assignable */,
                        new String[0] /* grantedRoles */);

    /**
     * Users capable of both reading and writing the store.
     */
    public static final RoleInstance READWRITE =
        new RoleInstance(READWRITE_NAME,
                        new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                                 SystemPrivilege.READ_ANY },
                        true /* assignable */,
                        new String[0] /* grantedRoles */);

    /**
     * Database administrator
     */
    public static final RoleInstance DBADMIN =
        new RoleInstance(DBADMIN_NAME,
                        new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                                 SystemPrivilege.DBVIEW },
                        true /* assignable */,
                        new String[0] /* grantedRoles */);

    /**
     * Store administrator
     */
    public static final RoleInstance SYSADMIN =
        new RoleInstance(SYSADMIN_NAME,
                        new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                                 SystemPrivilege.SYSOPER,
                                                 SystemPrivilege.SYSVIEW },
                        true /* assignable */,
                        new String[0] /* grantedRoles */);

    /**
     * A default role for all users. It could not be granted to/revoked from a
     * user or another role via the CLI commands.
     */
    public static final RoleInstance PUBLIC =
        new RoleInstance(PUBLIC_NAME,
                        new KVStorePrivilege[] { SystemPrivilege.USRVIEW,
                                                 SystemPrivilege.DBVIEW },
                        false /* assignable */,
                        new String[0] /* grantedRoles */);

    /**
     * A role for users that can modify system tables.
     */
    public static final RoleInstance WRITESYSTABLE =
        new RoleInstance(WRITESYSTABLE_NAME,
                         new KVStorePrivilege[] {
                             SystemPrivilege.WRITE_SYSTEM_TABLE },
                         true /* assignable */,
                         new String[0] /* grantedRoles */);

    /**
     * An internal role granted only to infrastructure components. It could not
     * be granted to/revoked from a user or another role, equivalent of R3
     * INTERNAL role.
     */
    public static final RoleInstance INTERNAL =
        new RoleInstance(KVStoreRole.INTERNAL.toString(),
                         new KVStorePrivilege[] { SystemPrivilege.INTLOPER },
                         false /* assignable */,
                         new String[] { PUBLIC.name(),
                                        SYSADMIN.name(),
                                        READWRITE.name(),
                                        WRITESYSTABLE.name() });

    /*
     * RoleInstance for R3 roles used to maintain compatibility during upgrade
     */

    /**
     * R3 role for KVStore admin user
     */
    public static final RoleInstance ADMIN =
        new RoleInstance(KVStoreRole.ADMIN.toString(),
                        new KVStorePrivilege[0],
                        false /* assignable */,
                        new String[] { PUBLIC.name(),
                                       SYSADMIN.name(),
                                       READWRITE.name() });

    /**
     *  R3 role for any authenticated user
     */
    public static final RoleInstance AUTHENTICATED =
        new RoleInstance(KVStoreRole.AUTHENTICATED.toString(),
                        new KVStorePrivilege[0],
                        false /* assignable */,
                        new String[] { PUBLIC.name(), READWRITE.name() });

    /* A name set of roles reserved for R3 compatibility */
    private static final Set<String> r3CompatRoleNames =
        new HashSet<String>(Arrays.asList(ADMIN.name(),
                                          INTERNAL.name(),
                                          AUTHENTICATED.name()));

    /**
     * Creates a readonly role using specified name and privileges.
     *
     * @param roleName
     * @param privileges
     * @param assignable
     * @param roles
     */
    private RoleInstance(String roleName,
                         KVStorePrivilege[] privileges,
                         boolean assignable,
                         String[] roles) {
        this(roleName, privileges, roles, assignable, true /* readonly */);
    }

    /**
     * Creates a generic kvstore role using specified name, privileges and
     * roles, as well as the assignable and readonly values.
     *
     * @param roleName
     * @param privileges
     * @param roles
     * @param assignable
     * @param readonly
     */
    public RoleInstance(String roleName,
                        KVStorePrivilege[] privileges,
                        String[] roles,
                        boolean assignable,
                        boolean readonly) {
        this.name = getNormalizedName(roleName);
        this.assignable = assignable;
        this.readonly = readonly;

        grantedPrivileges = new HashSet<KVStorePrivilege>();
        grantedRoles = new HashSet<String>();
        Collections.addAll(grantedPrivileges, privileges);
        Collections.addAll(grantedRoles, roles);
    }

    /**
     * Creates an assignable and non-readonly role with empty privilege and
     * empty role using specified role name.
     */
    public RoleInstance(String roleName) {
        this(roleName, new KVStorePrivilege[0],
             new String[0] /*roles*/, true /* assignable */,
             false /* readonly */);
    }

    /*
     * Copy ctor
     */
    protected RoleInstance(RoleInstance other) {
        super(other);
        this.name = other.name;
        this.assignable = other.assignable;
        this.readonly = other.readonly;
        this.grantedPrivileges =
            new HashSet<KVStorePrivilege>(other.grantedPrivileges);
        this.grantedRoles = new HashSet<String>(other.grantedRoles);
    }

    public RoleInstance(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        name = readString(in, serialVersion);
        grantedPrivileges =
            readCollection(in, serialVersion, HashSet::new,
                           KVStorePrivilege::readKVStorePrivilege);
        assignable = in.readBoolean();
        readonly = in.readBoolean();
        grantedRoles = readCollection(in, serialVersion,
                                      HashSet::new,
                                      SerializationUtil::readString);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        writeCollection(out, serialVersion, grantedPrivileges,
                        KVStorePrivilege::writeKVStorePrivilege);
        out.writeBoolean(assignable);
        out.writeBoolean(readonly);
        writeCollection(out, serialVersion, grantedRoles,
                        WriteFastExternal::writeString);
    }

    public String name() {
        return this.name;
    }

    /**
     * Grant privileges to this role instance.
     *
     * @param privileges
     */
    public RoleInstance
        grantPrivileges(Collection<KVStorePrivilege> privileges) {

        this.grantedPrivileges.addAll(privileges);
        return this;
    }

    /**
     * Grant roles to this role instance.
     *
     * @param roles
     */
    public RoleInstance grantRoles(Collection<String> roles) {
        for (final String role : roles) {
            grantedRoles.add(getNormalizedName(role));
        }
        return this;
    }

    /**
     * Revoke privileges from this role instance.
     *
     * @param privileges
     */
    public RoleInstance
        revokePrivileges(Collection<KVStorePrivilege> privileges) {

        this.grantedPrivileges.removeAll(privileges);
        return this;
    }

    /**
     * Revoke table privileges from this role instance.
     *
     * @param privileges
     */
    public RoleInstance
        revokeTablePrivileges(Collection<TablePrivilege> privileges) {

        this.grantedPrivileges.removeAll(privileges);
        return this;
    }

    /**
     * Revoke roles from this role instance.
     *
     * @param roles
     */
    public RoleInstance revokeRoles(Collection<String> roles) {
        for (final String role : roles) {
            grantedRoles.remove(getNormalizedName(role));
        }
        return this;
    }

    /**
     * Returns an immutable set of roles granted to the role.
     */
    public Set<String> getGrantedRoles() {
        return Collections.unmodifiableSet(grantedRoles);
    }

    /**
     * Returns granted roles in Json format.
     */
    String grantedRolesAsJSON() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (String role : grantedRoles) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append("\"");
            sb.append(role);
            sb.append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns an immutable set of privileges granted to the role.
     */
    public Set<KVStorePrivilege> getPrivileges() {
        return Collections.unmodifiableSet(grantedPrivileges);
    }

    /**
     * Returns granted privileges in Json format.
     */
    String grantedPrivilegesAsJSON() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (KVStorePrivilege priv : grantedPrivileges) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append("\"");
            sb.append(priv);
            sb.append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    public boolean assignable() {
        return assignable;
    }

    public boolean readonly() {
        return readonly;
    }

    public String toBriefString() {
        return String.format("name=%s", name);
    }

    /**
     * Returns the description of this role.
     *
     * @return role description information
     */
    public RoleDescription getDescription() {
        final String briefAsJSON =
            String.format("{\"name\":\"%s\"}",  name);
        final String detailsAsJSON =
            String.format("{\"name\":\"%s\", \"assignable\":\"%b\", " +
                          "\"readonly\":\"%b\",\"granted-privileges\":%s," +
                          "\"granted-roles\":%s}",
                          name, assignable, readonly, grantedPrivilegesAsJSON(),
                          grantedRolesAsJSON());
        return new RoleDescription(toBriefString(), briefAsJSON,
                                   toString(), detailsAsJSON);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        final RoleInstance other = (RoleInstance) obj;
        return name.equalsIgnoreCase(other.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public RoleInstance clone() {
        return new RoleInstance(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RoleInstance[");
        sb.append(" name=");
        sb.append(name);
        sb.append(" assignable=" + assignable);
        sb.append(" readonly=" + readonly);

        if (grantedRoles.size() > 0) {
            sb.append(" granted-roles=");
            sb.append(grantedRoles);
        }

        if (grantedPrivileges.size() > 0) {
            sb.append(" granted-privileges=");
            sb.append(grantedPrivileges);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public SecurityElementType getElementType() {
        return SecurityElementType.KVSTOREROLE;
    }

    /**
     * Returns a compatible name of role in a partially-upgraded store. For
     * those reserved R3 roles, the names in upper-case are returned since they
     * are used to construct an enum value in old version code. Otherwise, the
     * original name is returned, which is expected to be in lower case.
     */
    public static String getR3CompatName(String role) {
        if (r3CompatRoleNames.contains(role)) {
            return role.toUpperCase(ENGLISH);
        }
        return role;
    }

    /**
     * Returns a normalized role name. Currently implementation returns the
     * lower case result of the specified name.
     */
    public static String getNormalizedName(String role) {
        return role.toLowerCase(ENGLISH);
    }

    /**
     * A convenient class to store the description of a kvstore user-defined
     * role for showing.
     */
    public static class RoleDescription
            implements FastExternalizable, Serializable {

        private static final long serialVersionUID = 1L;

        private final String brief;
        private final String briefAsJSON;
        private final String details;
        private final String detailsAsJSON;

        public RoleDescription(String brief,
                               String briefAsJSON,
                               String details,
                               String detailsAsJSON) {
            this.brief = brief;
            this.briefAsJSON = briefAsJSON;
            this.details = details;
            this.detailsAsJSON = detailsAsJSON;
        }

        public RoleDescription(DataInput in, short serialVersion)
            throws IOException
        {
            brief = readString(in, serialVersion);
            briefAsJSON = readString(in, serialVersion);
            details = readString(in, serialVersion);
            detailsAsJSON = readString(in, serialVersion);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion, brief);
            writeString(out, serialVersion, briefAsJSON);
            writeString(out, serialVersion, details);
            writeString(out, serialVersion, detailsAsJSON);
        }

        /**
         * Gets the brief description.
         */
        public String brief() {
            return brief;
        }

        /**
         * Gets the brief description in JSON format.
         */
        public String briefAsJSON() {
            return briefAsJSON;
        }

        /**
         * Gets the detailed description.
         */
        public String details() {
            return details;
        }

        /**
         * Gets the detailed description in JSON format.
         */
        public String detailsAsJSON() {
            return detailsAsJSON;
        }
    }
}
