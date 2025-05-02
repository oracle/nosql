/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Tests oracle.kv.impl.security.PasswordHash.
 */
public class PasswordHashTest extends TestBase {

    static SecureRandom random = new SecureRandom();

    @Test
    public void testGenerateSalt() {

        /* Check error cases */

        try {
            /* bad result bytes */
            PasswordHash.generateSalt(random, 0);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        final int nBytes = PasswordHash.SUGG_SALT_BYTES;
        final byte[] salt = PasswordHash.generateSalt(random, nBytes);
        assertEquals(nBytes, salt.length);
    }

    @Test
    public void testPbeHash()
        throws InvalidKeySpecException, NoSuchAlgorithmException {

        final byte[] salt1 =
            PasswordHash.generateSalt(random,
                                      PasswordHash.SUGG_SALT_BYTES);

        final byte[] salt2 =
            PasswordHash.generateSalt(random,
                                      PasswordHash.SUGG_SALT_BYTES);


        /* check some error cases */
        try {
            /* null password */
            PasswordHash.pbeHash(null,
                                 PasswordHash.SUGG_ALGO,
                                 salt1,
                                 PasswordHash.SUGG_HASH_ITERS,
                                 16);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        try {
            /* empty password */
            PasswordHash.pbeHash("".toCharArray(),
                                 PasswordHash.SUGG_ALGO,
                                 salt1,
                                 PasswordHash.SUGG_HASH_ITERS,
                                 16);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        try {
            /* bad algorithm */
            PasswordHash.pbeHash("hello".toCharArray(),
                                 "NotAGoodAlgorithm",
                                 salt1,
                                 PasswordHash.SUGG_HASH_ITERS,
                                 16);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(NoSuchAlgorithmException.class, e.getClass());
        }

        try {
            /* null salt */
            PasswordHash.pbeHash("hello".toCharArray(),
                                 PasswordHash.SUGG_ALGO,
                                 null,
                                 PasswordHash.SUGG_HASH_ITERS,
                                 16);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        try {
            /* empty salt */
            PasswordHash.pbeHash("hello".toCharArray(),
                                 PasswordHash.SUGG_ALGO,
                                 new byte[0],
                                 PasswordHash.SUGG_HASH_ITERS,
                                 16);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        try {
            /* bad iters */
            PasswordHash.pbeHash("hello".toCharArray(),
                                 PasswordHash.SUGG_ALGO,
                                 salt1,
                                 0,
                                 16);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        try {
            /* bad result bytes */
            PasswordHash.pbeHash("hello".toCharArray(),
                                 PasswordHash.SUGG_ALGO,
                                 salt1,
                                 PasswordHash.SUGG_HASH_ITERS,
                                 0);
            fail("expected exception");
        } catch (Exception e) {
            assertSame(IllegalArgumentException.class, e.getClass());
        }

        final String[] passwords = {
            "abcdefg",
            "abcdefh",
            "abcdefgh" };

        final String algo = PasswordHash.SUGG_ALGO;
        final int nIters = PasswordHash.SUGG_HASH_ITERS;
        final int hashBytes = PasswordHash.SUGG_SALT_BYTES;

        for (int i = 0; i < passwords.length; i++) {
            final String pwdAStr = passwords[i];
            final char[] pwdA = pwdAStr.toCharArray();

            final byte[] hashA1 =
                PasswordHash.pbeHash(pwdA, algo, salt1, nIters, hashBytes);

            for (int j = 0; j < passwords.length; j++) {
                final String pwdBStr = passwords[j];
                final char[] pwdB = pwdBStr.toCharArray();

                final byte[] hashB1 = PasswordHash.pbeHash(
                    pwdB, algo, salt1, nIters, hashBytes);
                final byte[] hashB2 = PasswordHash.pbeHash(
                    pwdB, algo, salt2, nIters, hashBytes);
                final byte[] hashB1Iters = PasswordHash.pbeHash(
                    pwdB, algo, salt1, nIters + 1, hashBytes);
                final byte[] hashB1Bytes = PasswordHash.pbeHash(
                    pwdB, algo, salt1, nIters, hashBytes + 1);

                if (i == j) {
                    assertTrue(Arrays.equals(hashA1, hashB1));
                } else {
                    assertFalse(Arrays.equals(hashA1, hashB1));
                }

                assertFalse(Arrays.equals(hashA1, hashB2));
                assertFalse(Arrays.equals(hashA1, hashB1Iters));
                assertFalse(Arrays.equals(hashA1, hashB1Bytes));
            }
        }
    }
}
