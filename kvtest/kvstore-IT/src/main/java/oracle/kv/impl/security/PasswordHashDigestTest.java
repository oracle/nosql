/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.impl.security.metadata.PasswordHashDigest;

import org.junit.Test;

public class PasswordHashDigestTest extends TestBase {

    private static final SecureRandom random = new SecureRandom();

    @Test
    public void testGetHashDigest() {
        final byte[] salt = PasswordHash.generateSalt(
            random, PasswordHash.SUGG_SALT_BYTES);

        try {
            /* null algorithm */
            PasswordHashDigest.getHashDigest(null,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             salt,
                                             "pass".toCharArray());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* empty algorithm */
            PasswordHashDigest.getHashDigest("",
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             salt,
                                             "pass".toCharArray());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* unknown algorithm */
            PasswordHashDigest.getHashDigest("UNKNOWN_ALGO",
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             salt,
                                             "pass".toCharArray());
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ise) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* bad iterations */
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             0,
                                             16,
                                             salt,
                                             "pass".toCharArray());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* bad hashBytes */
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             0,
                                             salt,
                                             "pass".toCharArray());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* null salt */
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             null,
                                             "pass".toCharArray());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* empty salt */
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             new byte[0],
                                             "pass".toCharArray());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* null password */
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             salt,
                                             null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        try {
            /* empty password */
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             salt,
                                             new char[0]);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */
    }

    @Test
    public void testLifeTime() {
        final byte[] salt = PasswordHash.generateSalt(
            random, PasswordHash.SUGG_SALT_BYTES);
        final PasswordHashDigest pwdDigest =
            PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                             PasswordHash.SUGG_HASH_ITERS,
                                             16,
                                             salt,
                                             "pass".toCharArray());


        /* Lifetime is 0 by default, and thus never expires */
        assertFalse(pwdDigest.isExpired());

        /* Negative value makes it expired */
        pwdDigest.setLifetime(-1L);
        assertTrue(pwdDigest.isExpired());

        /* Set lifetime as 2 secs */
        pwdDigest.setLifetime(TimeUnit.SECONDS.toMillis(2));
        assertFalse(pwdDigest.isExpired());

        /* Should expire after 2 secs */
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            fail("Unexpected exception");
        }
        assertTrue(pwdDigest.isExpired());
    }
}
