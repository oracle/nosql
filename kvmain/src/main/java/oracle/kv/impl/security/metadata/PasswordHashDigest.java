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

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import oracle.kv.impl.security.PasswordHash;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.FormatUtils;

/**
 * Class for storing the password hashing information.
 */
public final class PasswordHashDigest
        implements FastExternalizable, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    /* Salt value for hashing */
    private final byte[] saltValue;

    /* Hash algorithm for hashing the password */
    private final String hashAlgorithm;

    /* Iterations in hashing the password */
    private final int hashIterations;

    /* Hashed password for the user */
    private final byte[] hashedPassword;

    /* Length of bytes of salt value and password hash */
    private final int hashBytes;

    /* Creation time stamp for checking if this password is expired */
    private long createdTimeInMillis = System.currentTimeMillis();

    /*
     * Life time for the password. It's unlimited if set to 0, or it is expired
     * if set to a negative value.
     */
    private long lifetime = 0L;

    private PasswordHashDigest(final String hashAlgo,
                               final byte[] saltValue,
                               final int hashIters,
                               final byte[] hashedPasswd) {
        this.hashAlgorithm = hashAlgo;
        this.saltValue = saltValue;
        this.hashIterations = hashIters;
        this.hashBytes = hashedPasswd.length;
        this.hashedPassword = hashedPasswd;
    }

    PasswordHashDigest(final DataInput in, final short serialVersion)
        throws IOException
    {
        saltValue = readByteArray(in);
        hashAlgorithm = readString(in, serialVersion);
        hashIterations = in.readInt();
        hashedPassword = readByteArray(in);
        hashBytes = in.readInt();
        createdTimeInMillis = in.readLong();
        lifetime = in.readLong();
    }

    @Override
    public void writeFastExternal(final DataOutput out, final short sv)
        throws IOException
    {
        writeByteArray(out, saltValue);
        writeString(out, sv, hashAlgorithm);
        out.writeInt(hashIterations);
        writeByteArray(out, hashedPassword);
        out.writeInt(hashBytes);
        out.writeLong(createdTimeInMillis);
        out.writeLong(lifetime);
    }

    /**
     * Verify whether the plain password has the same hashed value with the
     * stored hashed password when encrypted with stored parameters.
     *
     * @param plainPassword
     * @return true if matches
     */
    public boolean verifyPassword(final char[] plainPassword) {
        final byte[] newHashedPassword =
                getHashDigest(hashAlgorithm, hashIterations, hashBytes,
                              saltValue, plainPassword).hashedPassword;
        return Arrays.equals(this.hashedPassword, newHashedPassword);
    }

    /**
     * Get a PasswordHashDigest instance with full parameters.
     *
     * @param hashAlgorithm hash algorithm
     * @param hashIterations hash iterations
     * @param hashBytes length of bytes of salt value and hashed password
     * @param saltValue the salt value
     * @param plainPassword plain password
     * @return a PasswordHashDigest containing the hashed password and hashing
     * information
     */
    public static PasswordHashDigest getHashDigest(final String hashAlgorithm,
                                                   final int hashIterations,
                                                   final int hashBytes,
                                                   final byte[] saltValue,
                                                   final char[] plainPassword) {
        if ((hashAlgorithm == null) || hashAlgorithm.isEmpty()) {
            throw new IllegalArgumentException(
                "Hash algorithm must not be null or empty.");
        }
        if (hashIterations <= 0) {
            throw new IllegalArgumentException(
                "Hash iterations must be a non-negative integer: " +
                hashIterations);
        }
        if (hashBytes <= 0) {
            throw new IllegalArgumentException(
                "Hash bytes must be a non-negative integer: " + hashBytes);
        }
        if (saltValue == null || saltValue.length == 0) {
            throw new IllegalArgumentException(
                    "Salt value must not be null or empty.");
        }
        if (plainPassword == null || plainPassword.length == 0) {
            throw new IllegalArgumentException(
                    "Plain password must not be null or empty.");
        }

        Exception hashException = null;
        try {
            final byte[] hashedPasswd =
                    PasswordHash.pbeHash(plainPassword, hashAlgorithm,
                                         saltValue, hashIterations, hashBytes);

            return new PasswordHashDigest(hashAlgorithm, saltValue,
                                          hashIterations, hashedPasswd);

        } catch (NoSuchAlgorithmException e) {
            hashException = e;
        } catch (InvalidKeySpecException e) {
            hashException = e;
        }
        throw new IllegalStateException(
            "Failed to get hashed password.", hashException);
    }

    /**
     * Return if the password is expired.
     */
    public boolean isExpired() {
        if (lifetime == 0) {
            return false;
        }
        return lifetime < 0 ?
               true :
               System.currentTimeMillis() >= (createdTimeInMillis + lifetime);
    }

    /**
     * Return the expiration info of this password.
     * @return expiration date time in format of yyyy-MM-dd HH:mm:ss z of
     * this password or <code>non-expiring</code> if the lifetime is 0 or
     * <code>expired</code> if the password is expired.
     */
    public String getExpirationInfo() {
        if (lifetime == 0) {
            return "non-expiring";
        } else if (isExpired()) {
            return "expired";
        }
        return FormatUtils.formatDateTime(createdTimeInMillis + lifetime);
    }

    public long getLifetime() {
        return lifetime;
    }

    /**
     * Configure the lifetime in millisecond for this password.
     *
     * @param newLifetime lifetime of this password in milliseconds.  If
     * being set to values less than 0, the password will be expired. If being
     * set to 0, the password will never expired.
     */
    public void setLifetime(final long newLifetime) {
        this.lifetime = newLifetime;
    }

    /*
     * Refreshes the created time to current.
     */
    public void refreshCreateTime() {
        createdTimeInMillis = System.currentTimeMillis();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + hashIterations;
        result = result * prime + hashAlgorithm.hashCode();
        result = result * prime + Arrays.hashCode(saltValue);
        result = result * prime + Arrays.hashCode(hashedPassword);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PasswordHashDigest)) {
            return false;
        }
        final PasswordHashDigest other = (PasswordHashDigest) obj;
        return Arrays.equals(hashedPassword, other.hashedPassword) &&
               Arrays.equals(saltValue, other.saltValue) &&
               hashAlgorithm.equals(other.hashAlgorithm) &&
               hashIterations == other.hashIterations &&
               hashBytes == other.hashBytes;
    }

    @Override
    public PasswordHashDigest clone() {
        final PasswordHashDigest result =
                new PasswordHashDigest(hashAlgorithm, saltValue.clone(),
                                       hashIterations, hashedPassword.clone());
        result.createdTimeInMillis = createdTimeInMillis;
        result.lifetime = lifetime;
        return result;
    }
}
