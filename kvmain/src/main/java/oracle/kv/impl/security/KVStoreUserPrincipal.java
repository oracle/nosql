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

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import javax.security.auth.Subject;

/**
 * Represents the identity of a KVStore user.
 */
public class KVStoreUserPrincipal implements KVStorePrincipal, Serializable {

    private static final long serialVersionUID = 1L;
    private final String username;
    private final String userId;

    public static final String INTERNAL_NAME = "_internal_";

    public static final KVStoreUserPrincipal INTERNAL =
        new KVStoreUserPrincipal(INTERNAL_NAME);

    /**
     * Constructs a user principal object for a "user" with no id.
     * The obvious case of this is the internal user.
     *
     * @param username the name of the user - must be non-null
     * @throws IllegalArgumentException if the username is null
     */
    public KVStoreUserPrincipal(String username) {
        this(username, null);
    }

    /**
     * Constructs a user principal object.
     *
     * @param username the name of the user - must be non-null
     * @param userId the id of the user - null allowable
     * @throws IllegalArgumentException if the username is null
     */
    public KVStoreUserPrincipal(String username, String userId) {
        if (username == null) {
            throw new IllegalArgumentException("username may not be null");
        }
        this.username = username;
        this.userId = userId;
    }

    public KVStoreUserPrincipal(DataInput in, short serialVersion)
        throws IOException
    {
        username = readString(in, serialVersion);
        userId = readString(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writeString(out, serialVersion, username);
        writeString(out, serialVersion, userId);
    }

    @Override
    public PrincipalType getPrincipalType() {
        return PrincipalType.USER;
    }

    /**
     * Get the userID of the user represented by this principal.
     * If the user is not a registered user (e.g. the internal KVStore
     * infrastructure user), this will be null.  If not null, if can be
     * used to lookup up a KVStoreUser.
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Get the principal associated with the currently active user.
     * @return the active user principal
     */
    public static KVStoreUserPrincipal getCurrentUser() {

        return ExecutionContext.getCurrentUserPrincipal();
    }

    /**
     * Get the principal associated with the specified Subject.
     * @return the user principal
     */
    public static KVStoreUserPrincipal getSubjectUser(Subject subj) {

        return ExecutionContext.getSubjectUserPrincipal(subj);
    }

    /*
     * Principal interface methods
     */

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return username.equals(((KVStoreUserPrincipal) other).username);
    }

    @Override
    public String getName() {
        return username;
    }

    @Override
    public int hashCode() {
        return username.hashCode();
    }

    @Override
    public String toString() {
        return "KVStoreUserPrincipal(" + username + "," + userId + ")";
    }

}
