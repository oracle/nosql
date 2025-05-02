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

package oracle.kv.impl.security.metadata;

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.impl.security.KVStorePrincipal;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;
import oracle.kv.impl.security.util.SubjectUtils;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;

/**
 * KVStore user definition. Note that external users don't support password
 * operation.
 */
public class KVStoreUser extends SecurityMetadata.SecurityElement {

    private static final long serialVersionUID = 1L;

    /**
     * Default roles of general user created in R3.0
     */
    private static final Set<String> USER_V1_DEFAULT_ROLES;
    /**
     * Default roles of Admin user created in R3.0
     */
    private static final Set<String> ADMIN_V1_DEFAULT_ROLES;

    static {
        /* Add general user default roles */
        final String[] userV1RoleNames =
            new String[] { RoleInstance.PUBLIC_NAME,
                           RoleInstance.READWRITE_NAME };
        USER_V1_DEFAULT_ROLES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(userV1RoleNames)));

        /* Add Admin user default roles */
        final String[] adminV1RoleNames =
            new String[] { RoleInstance.SYSADMIN_NAME,
                           RoleInstance.READWRITE_NAME,
                           RoleInstance.PUBLIC_NAME };
        ADMIN_V1_DEFAULT_ROLES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(adminV1RoleNames)));
    }

    /**
     * User types, new types must be added to the end of this list.
     */
    public enum UserType implements FastExternalizable {
        LOCAL(0),
        EXTERNAL(1);
        private static final UserType[] VALUES = values();
        UserType(final int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }
        static UserType readFastExternal(DataInput in,
                                         @SuppressWarnings("unused") short sv)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown UserType: " + ordinal);
            }
        }
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            out.writeByte(ordinal());
        }
    }

    /** Which KVStoreUser subclass to use. */
    enum SubtypeKey implements FastExternalizable {
        V1(0, KVStoreUser::new),
        V2(1, KVStoreUserV2::new);
        private static final SubtypeKey[] VALUES = values();
        private final ReadFastExternal<KVStoreUser> reader;
        SubtypeKey(final int ordinal,
                   final ReadFastExternal<KVStoreUser> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }
        static SubtypeKey readFastExternal(DataInput in,
                                           @SuppressWarnings("unused")
                                           short sv)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown SubtypeKey: " + ordinal);
            }
        }
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            out.writeByte(ordinal());
        }
        KVStoreUser readUser(DataInput in, short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }
    }

    final String userName;

    private UserType userType = UserType.LOCAL;

    /* Used as the main password in authentication  */
    private PasswordHashDigest primaryPassword;

    /*
     * This password is mainly used during password updating procedure, and is
     * intended for letting the new and old password take effect simultaneously
     * in a specified period for authentication.
     */
    private PasswordHashDigest retainedPassword;

    /*
     * Store the hash digest of previous passwords. The maximum number of
     * previous passwords stored is 256. If the number of stored passwords
     * exceed the limit, the oldest one will be removed from the list.
     */
    private SizedPrevPasswordList rememberedPasswords;

    /*
     * Whether the user is enabled. A user is active and is able to login the
     * system only when it is enabled.
     */
    private boolean enabled;

    /* Whether the user is an Admin */
    boolean isAdmin;

    /**
     * Create a new KVStoreUser.
     */
    public static KVStoreUser newInstance(final String name) {
        return new KVStoreUserV2(name);
    }

    /* For unit test */
    public static KVStoreUser newV1Instance(final String name) {
        return new KVStoreUser(name);
    }

    /**
     * Create an initial user instance with specified name, without password
     * and is not yet enabled.
     */
    private KVStoreUser(final String name) {
        this.userName = name;
    }

    /*
     * Copy ctor
     */
    private KVStoreUser(final KVStoreUser other) {
        super(other);
        userName = other.userName;
        userType = other.userType;
        enabled = other.enabled;
        isAdmin = other.isAdmin;

        primaryPassword = other.primaryPassword == null ?
                          null : other.primaryPassword.clone();
        retainedPassword = other.retainedPassword == null ?
                           null : other.retainedPassword.clone();
        rememberedPasswords = other.rememberedPasswords == null ?
                              null : other.rememberedPasswords.clone();
    }

    public KVStoreUser(final DataInput in, final short serialVersion)
        throws IOException
    {
        super(in, serialVersion);
        userName = readString(in, serialVersion);
        userType = UserType.readFastExternal(in, serialVersion);
        primaryPassword = readFastExternalOrNull(in, serialVersion,
                                                 PasswordHashDigest::new);
        retainedPassword = readFastExternalOrNull(in, serialVersion,
                                                  PasswordHashDigest::new);
        rememberedPasswords = readFastExternalOrNull(
            in, serialVersion, SizedPrevPasswordList::new);
        enabled = in.readBoolean();
        isAdmin = in.readBoolean();
    }

    @Override
    public void writeFastExternal(final DataOutput out, final short sv)
        throws IOException
    {
        super.writeFastExternal(out, sv);
        writeString(out, sv, userName);
        userType.writeFastExternal(out, sv);
        writeFastExternalOrNull(out, sv, primaryPassword);
        writeFastExternalOrNull(out, sv, retainedPassword);
        writeFastExternalOrNull(out, sv, rememberedPasswords);
        out.writeBoolean(enabled);
        out.writeBoolean(isAdmin);
    }

    /** Reads a user instance subtype. */
    static KVStoreUser readUser(final DataInput in, final short serialVersion)
        throws IOException
    {
        return SubtypeKey.readFastExternal(in, serialVersion)
            .readUser(in, serialVersion);
    }

    /** Writes this user instance subtype. */
    void writeUser(final DataOutput out, final short serialVersion)
        throws IOException
    {
        getSubtypeKey().writeFastExternal(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }

    /** Gets the subtype key of this user object. */
    SubtypeKey getSubtypeKey() {
        return SubtypeKey.V1;
    }

    /**
     * Sets the type of user. The valid types are defined as in
     * {@link UserType}.
     *
     * @param type type of user
     * @return this
     */
    public KVStoreUser setUserType(final UserType type) {
        this.userType = type;
        return this;
    }

    /**
     * Gets the type of the user.
     *
     * @return user type defined as in {@link UserType}
     */
    public UserType getUserType() {
        return userType;
    }

    /**
     * Gets the name of the user.
     *
     * @return user name
     */
    public String getName() {
        return userName;
    }

    /**
     * Save the encrypted password of the user. The password will be used as
     * the primary one in authentication.
     *
     * @param primaryPasswd the primary password
     * @return this
     */
    public KVStoreUser setPassword(final PasswordHashDigest primaryPasswd) {
        if (this.userType == UserType.EXTERNAL) {
            throw new IllegalStateException("Cannnot set password " +
                "for external user");
        }
        primaryPassword = primaryPasswd;
        if (rememberedPasswords == null) {
            rememberedPasswords = new SizedPrevPasswordList();
        }
        rememberedPasswords.add(primaryPasswd);
        return this;
    }

    /**
     * Configure the current primary password lifetime.
     *
     * @param amount lifetime of primary password in milliseconds
     * @return this
     */
    public KVStoreUser setPasswordLifetime(final long amount) {
        if (this.userType == UserType.EXTERNAL) {
            throw new IllegalStateException("Cannnot set password lifetime " +
                "for external user");
        }
        primaryPassword.setLifetime(amount);
        return this;
    }

    /**
     * Retains the current primary password as a secondary password during the
     * password changing operation. This enables users to login using both new
     * and old passwords.
     *
     * @return this
     */
    public KVStoreUser retainPassword() {

        if (this.userType == UserType.EXTERNAL) {
            throw new IllegalStateException("Cannnot retain password " +
                "for external user");
        }
        /* Retained password could not be overridden. */
        if (retainedPasswordValid()) {
            throw new IllegalStateException(
                "Could not override an existing retained password.");
        }
        retainedPassword = primaryPassword;
        retainedPassword.refreshCreateTime();
        return this;
    }

    /**
     * Gets the primary password of the user.
     *
     * @return a PasswordHashDigest object containing the primary password
     */
    public PasswordHashDigest getPassword() {
        return primaryPassword;
    }

    /**
     * Gets the retained secondary password of the user.
     *
     * @return a PasswordHashDigest object containing the secondary password
     */
    public PasswordHashDigest getRetainedPassword() {
        return retainedPassword;
    }

    public PasswordHashDigest[] getRememberedPasswords(int number) {
        if (rememberedPasswords != null) {
            return rememberedPasswords.getRememberedPasswords(number);
        }
        return null;
    }

    /**
     * A wrapped linked list with max size. Used to store the previous password
     * hash digest information. The maximum size is set to 256. The maximum
     * allowed to set value for retrieving previous remembered passwords is
     * 256 as well. The size will allow the previous remembered password check
     * be usable, on the same time it does not over capacity for the metadata
     * storage. When the size of the internal linked list is over the maximum.
     * The oldest element from the list will be removed in order to maintain
     * the size of the list.
     */
    private class SizedPrevPasswordList
            implements FastExternalizable, Serializable, Cloneable {

        private static final long serialVersionUID = 1L;

        /*
         * The maximum number of password to be remembered
         */
        private static final int MAX_REMEMBER = 256;

        /*
         * Used to save the previous password
         */
        private final LinkedList<PasswordHashDigest> prevPassList;

        private SizedPrevPasswordList() {
            prevPassList = new LinkedList<>();
        }

        private SizedPrevPasswordList(DataInput in, short serialVersion)
            throws IOException
        {
            prevPassList = readCollection(in, serialVersion, LinkedList::new,
                                          PasswordHashDigest::new);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeCollection(out, serialVersion, prevPassList);
        }

        private synchronized void add(PasswordHashDigest element) {
            while (prevPassList.size() >= MAX_REMEMBER) {
                prevPassList.remove();
            }
            prevPassList.add(element.clone());
        }

        /*
         * Return given number of latest previous password hash digest copies.
         */
        public synchronized PasswordHashDigest[]
            getRememberedPasswords(int number) {
            final int targetNumber =
                (prevPassList.size() >= number) ? number : prevPassList.size();
            final PasswordHashDigest[] results =
                new PasswordHashDigest[targetNumber];
            int counter = 0;
            final Iterator<PasswordHashDigest> iter =
                prevPassList.descendingIterator();
            while (iter.hasNext()) {
                PasswordHashDigest phd = iter.next();
                results[counter] = phd.clone();
                if (++counter == targetNumber) {
                    break;
                }
            }
            return results;
        }

        @Override
        public synchronized SizedPrevPasswordList clone() {
            final SizedPrevPasswordList newList =
                new SizedPrevPasswordList();
            for (PasswordHashDigest element : prevPassList) {
                newList.add(element.clone());
            }
            return newList;
        }
    }

    /**
     * Clears the current retained secondary password.
     */
    public void clearRetainedPassword() {
        retainedPassword = null;
    }

    /**
     * Checks if the user is in enabled state.
     *
     * @return true if enabled, otherwise false.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Checks if the user is an administrator, who has sysadmin role.
     *
     * @return true if user has sysadmin role, otherwise false.
     */
    public boolean isAdmin() {
        return getGrantedRoles().contains(RoleInstance.SYSADMIN_NAME);
    }

    /**
     * Checks if the retained password is valid. The retained password is valid
     * iff. it is not null and not expired.
     */
    public boolean retainedPasswordValid() {
        return (retainedPassword != null) && (!retainedPassword.isExpired());
    }

    /**
     * Marks the user as an Admin or not.
     *
     * @param flag whether to be an admin
     * @return this
     */
    public KVStoreUser setAdmin(final boolean flag) {
        this.isAdmin = flag;
        return this;
    }

    /**
     * Marks the user as enabled or not.
     *
     * @param flag whether to be enabled
     * @return this
     */
    public KVStoreUser setEnabled(final boolean flag) {
        this.enabled = flag;
        return this;
    }

    /**
     * Get both brief and detailed description of a user for showing.
     *
     * @return a pair of {@literal <brief, details>} information
     */
    public UserDescription getDescription() {
        final boolean rPassActive = retainedPasswordValid();
        final String retainInfo;
        if (rPassActive) {
            retainInfo = String.format("active [expiration: %s]",
                                       retainedPassword.getExpirationInfo());
        } else {
            retainInfo = "inactive";
        }
        final String briefAsJSON =
            String.format("{\"id\":\"%s\", \"name\":\"%s\"}",
                          super.getElementId(), userName);
        final String retainField =
            userType == UserType.EXTERNAL ? "" : " retain-passwd=" + retainInfo;
        final String retainFieldAsJson =
            userType == UserType.EXTERNAL ? "" : "\"retain-passwd\":\"" +
                retainInfo +"\", ";

        final String pwdExpiryField;
        final String pwdExpiryFieldAsJson;
        if (userType == UserType.EXTERNAL) {
            pwdExpiryField = "";
            pwdExpiryFieldAsJson = "";
        } else {
            final String pwdExpiryInfo = primaryPassword.getExpirationInfo();
            pwdExpiryField = " current-passwd-expiration=" + pwdExpiryInfo;
            pwdExpiryFieldAsJson = "\"current-passwd-expiration\":\"" +
                pwdExpiryInfo +"\", ";
        }

        final String details =
            String.format("%s enabled=%b auth-type=%s" + pwdExpiryField +
                          retainField + " granted-roles=%s",
                          toString(), enabled, userType, getGrantedRoles());
        final String detailsAsJSON =
            String.format("{\"id\":\"%s\", \"name\":\"%s\", \"enabled\":" +
                          "\"%b\", \"type\":\"%s\", " + pwdExpiryFieldAsJson +
                          retainFieldAsJson + "\"granted-roles\":%s}",
                          getElementId(), userName, enabled, userType,
                          grantedRolesAsJSON());
        return new UserDescription(toString(), briefAsJSON, details,
                                   detailsAsJSON);
    }

    /**
     * Grant roles to user.  A new copy of this user with newly granted roles
     * will be returned.
     */
    public KVStoreUser grantRoles(Collection<String> roles) {

        return new KVStoreUserV2(this).grantRoles(roles);
    }

    /**
     * Revoke roles from user. A new copy of this user with updated roles will
     * be returned.
     */
     public KVStoreUser revokeRoles(Collection<String> roles) {

        return new KVStoreUserV2(this).revokeRoles(roles);
    }

     /**
      * Return the roles granted to this user.
      */
     public Set<String> getGrantedRoles() {
         if (isAdmin) {
             return ADMIN_V1_DEFAULT_ROLES;
         }

         return USER_V1_DEFAULT_ROLES;
     }

     private String grantedRolesAsJSON() {
         final StringBuilder sb = new StringBuilder();
         sb.append("[");
         boolean first = true;
         for (String role : getGrantedRoles()) {
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
     * Verifies if the plain password matches with the password of the user.
     *
     * @param password the plain password
     * @return true iff. all the following conditions holds:
     * <li>the user is enabled, and</li>
     * <li>the primary password matches with the plain password, or the
     * retained password is valid and matches with the plain password. </li>
     */
    public boolean verifyPassword(final char[] password) {
        if (this.userType == UserType.EXTERNAL) {
            throw new IllegalStateException("Cannnot verify password " +
                "for external user");
        }
        if (password == null || password.length == 0) {
            return false;
        }

        if (!isEnabled()) {
            return false;
        }
        return getPassword().verifyPassword(password) ||
            (retainedPasswordValid() &&
             getRetainedPassword().verifyPassword(password));
    }

    /**
     * Return if the primary password expire.
     */
    public boolean isPasswordExpired() {
        if (this.userType == UserType.EXTERNAL) {
            throw new IllegalStateException("Cannnot determine the password" +
                " expiration for external user");
        }
        return primaryPassword.isExpired();
    }

    /**
     * Creates a Subject with the KVStoreRolePrincipals and
     * KVStoreUserPrincipals indicated by this entry.
     *
     * @return a newly created Subject
     */
    public Subject makeKVSubject() {
        final String userId = getElementId();
        final Set<KVStorePrincipal> userPrincipals = new HashSet<>();

        /* Use old R3 role principles make subject during upgrade */
        userPrincipals.add(KVStoreRolePrincipal.AUTHENTICATED);
        if (isAdmin) {
            userPrincipals.add(KVStoreRolePrincipal.ADMIN);
        }
        userPrincipals.add(new KVStoreUserPrincipal(userName, userId));

        return SubjectUtils.createSubject(userPrincipals);
    }

    @Override
    public SecurityElementType getElementType() {
        return SecurityElementType.KVSTOREUSER;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        final int result =
            17 * prime + (userName == null ? 0 : userName.hashCode());
        return result;
    }

    /**
     * Two KVStoreUsers are identical iff. they have the same names and ids.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KVStoreUser)) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        final KVStoreUser other = (KVStoreUser) obj;
        if (userName == null) {
            return (other.userName == null);
        }
        return userName.equals(other.userName);
    }

    @Override
    public String toString() {
        return String.format("KVStoreUser[id=%s name=%s]",
                             super.getElementId(), userName);
    }

    @Override
    public KVStoreUser clone() {
        return new KVStoreUser(this);
    }


    /**
     * A convenient class to store the description of a kvstore user for
     * showing. With this class we do not need to pass the full KVStoreUser
     * copy to client for showing, avoiding the security risk.
     */
    public static class UserDescription implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String brief;
        private final String briefAsJSON;
        private final String details;
        private final String detailsAsJSON;

        public UserDescription(String brief,
                               String briefAsJSON,
                               String details,
                               String detailsAsJSON) {
            this.brief = brief;
            this.briefAsJSON = briefAsJSON;
            this.details = details;
            this.detailsAsJSON = detailsAsJSON;
        }

        /**
         * Gets the brief description.
         *
         * @return briefs
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
         *
         * @return details
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

    /**
     * Define a subclass of KVStoreUser with non-default roles.
     */
    static class KVStoreUserV2 extends KVStoreUser {
        private static final long serialVersionUID = 1L;
        private final Set<String> grantedRoles;

        private KVStoreUserV2(String name) {
            super(name);
            grantedRoles = new HashSet<>();

            /* Grant PUBLIC role to any user by default */
            grantedRoles.add(RoleInstance.PUBLIC_NAME);
        }

        /*
         * Construct a V2 KVStoreUser from an V1 KVStoreUser.
         */
        private KVStoreUserV2(final KVStoreUser other) {
            super(other);
            grantedRoles = new HashSet<>(other.getGrantedRoles());
        }

        private KVStoreUserV2(DataInput in, short serialVersion)
            throws IOException
        {
            super(in, serialVersion);
            grantedRoles = readCollection(in, serialVersion, HashSet::new,
                                          SerializationUtil::readString);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            super.writeFastExternal(out, serialVersion);
            writeCollection(out, serialVersion, grantedRoles,
                            WriteFastExternal::writeString);
        }

        @Override
        SubtypeKey getSubtypeKey() {
            return SubtypeKey.V2;
        }

        @Override
        public KVStoreUserV2 setAdmin(final boolean flag) {
            if (flag != isAdmin()) {
                isAdmin = flag;
                if (isAdmin) {
                    /* Grant SYSADMIN role to Admin user by default */
                    grantedRoles.add(RoleInstance.SYSADMIN_NAME);
                } else {
                    /* Revoke SYSADMIN role from Admin user by default */
                    grantedRoles.remove(RoleInstance.SYSADMIN_NAME);
                }
            }
            return this;
        }

        @Override
        public KVStoreUserV2 grantRoles(Collection<String> roles) {
            for (final String role : roles) {
                grantedRoles.add(RoleInstance.getNormalizedName(role));
            }
            return this;
        }

        @Override
        public KVStoreUserV2 revokeRoles(Collection<String> roles) {

            /*
             * Do not check if user has the given roles to be revoked in
             * order to avoid role name information exposure.
             * */
            for (final String role : roles) {
                grantedRoles.remove(RoleInstance.getNormalizedName(role));
            }
            return this;
        }

        @Override
        public Set<String> getGrantedRoles() {
            return Collections.unmodifiableSet(grantedRoles);
        }

        @Override
        public KVStoreUserV2 clone() {
            return new KVStoreUserV2(this);
        }

        @Override
        public Subject makeKVSubject() {
            final String userId = getElementId();
            final Set<KVStorePrincipal> userPrincipals = new HashSet<>();
            for (String role : getGrantedRoles()) {
                KVStoreRolePrincipal princ = KVStoreRolePrincipal.get(role);
                userPrincipals.add(princ);
            }
            userPrincipals.add(new KVStoreUserPrincipal(userName, userId));

            return SubjectUtils.createSubject(userPrincipals);
        }
    }
}
