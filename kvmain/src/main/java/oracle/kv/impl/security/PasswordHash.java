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

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Class to perform password hashing.
 */
public final class PasswordHash {
    /**
     * A suggested algorithm to use for hash computation.
     */
    public static final String SUGG_ALGO = "PBKDF2WithHmacSHA256";

    /**
     * A suggested number of salt bytes to use.
     */
    public static final int SUGG_SALT_BYTES = 16;

    /**
     * A suggested number of hash iterations to use. This yields a hash
     * computation time of about 10ms
     */
    public static final int SUGG_HASH_ITERS = 5000;

    /*
     * Not instantiable
     */
    private PasswordHash() {
    }

    /**
     * Compute the PBE hash of a password.
     *
     * @param password the password to hash. must be non-null and non-empty
     * @param algorithm the algorithm to use
     * @param salt the salt. Must be non-null and non-empty
     * @param iters the iteration count. Must be positive
     * @param bytes the length of the hash to compute in bytes. Must be positive
     * @return the PBE hash of the password
     * @throws NoSuchAlgorithmException if the specified algorithm is unknown
     * @throws InvalidKeySpecException if the specified parameters are invalid
     *        for the algorithm
     * @throws IllegalArgumentException if the salt is null or empty, if the
     *        password is null or empty, or if bytes is &lt;= 0, or if iters
     *        is &lt;= 0
     */
    public static byte[] pbeHash(char[] password,
                                 String algorithm,
                                 byte[] salt,
                                 int iters,
                                 int bytes)
        throws NoSuchAlgorithmException, InvalidKeySpecException,
               IllegalArgumentException {

        if (password == null || password.length == 0) {
            throw new IllegalArgumentException(
                "The password must be non-null and not empty");
        }

        if (salt == null || salt.length == 0) {
            throw new IllegalArgumentException(
                "The salt must be non-null and not empty");
        }

        if (iters <= 0) {
            throw new IllegalArgumentException(
                "The number of iterations must be > 0");
        }

        if (bytes <= 0) {
            throw new IllegalArgumentException(
                "The number of result bytes must be > 0");
        }

        final PBEKeySpec spec =
            new PBEKeySpec(password, salt, iters, bytes * 8);
        final SecretKeyFactory skf =
            SecretKeyFactory.getInstance(algorithm);
        return skf.generateSecret(spec).getEncoded();
    }

    /**
     * Generate a random salt value.
     *
     * @param random a SecureRandom instance
     * @param nBytes the number of bytes of salt to generate
     * @return the generated salt
     * @throws IllegalArgumentException if nBytes is &lt;= 0
     */
    public static byte[] generateSalt(SecureRandom random, int nBytes)
        throws IllegalArgumentException {

        if (nBytes <= 0) {
            throw new IllegalArgumentException(
                "The number of result bytes must be > 0");
        }

        final byte[] salt = new byte[nBytes];
        random.nextBytes(salt);
        return salt;
    }
}
