/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.TestBase;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

import org.junit.Test;

/**
 * Test the TokenVerifier class.
 */
public class TokenVerifierTest extends TestBase {

    private static int counter = 0;

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testVerifyWithCache() {

        final CacheConfig cacheConfig =
            new CacheConfig().setCapacity(100).setLifetime(3600 * 1000L);
        final DummyResolver resolver = new DummyResolver();

        final TokenVerifier verifier = new TokenVerifier(cacheConfig, resolver);

        final LoginToken okToken = resolver.getOkToken();
        final LoginToken badToken = makeLoginToken();

        final int nTimes = 10;

        for (int i = 0; i < nTimes; i++) {
            final Subject subj1 = verifier.verifyToken(okToken);
            assertNotNull(subj1);

            final Subject subj2 = verifier.verifyToken(badToken);
            assertNull(subj2);
        }

        assertEquals(1, resolver.getSuccessfulResolves());
        assertEquals(nTimes, resolver.getFailedResolves());
    }

    @Test
    public void testVerifyWithoutCache() {

        final DummyResolver resolver = new DummyResolver();

        final TokenVerifier verifier = new TokenVerifier(null, resolver);

        final LoginToken okToken = resolver.getOkToken();
        final LoginToken badToken = makeLoginToken();

        final int nTimes = 10;

        for (int i = 0; i < nTimes; i++) {
            final Subject subj1 = verifier.verifyToken(okToken);
            assertNotNull(subj1);

            final Subject subj2 = verifier.verifyToken(badToken);
            assertNull(subj2);
        }

        assertEquals(nTimes, resolver.getSuccessfulResolves());
        assertEquals(nTimes, resolver.getFailedResolves());
    }

    private LoginToken makeLoginToken() {
        final byte[] sid = new byte[1];
        sid[0] = (byte) counter++;

        return new LoginToken(
            new SessionId(sid), System.currentTimeMillis() + 3600 * 1000L);
    }


    class DummyResolver implements TokenResolver {

        private int successfulResolves;
        private int failedResolves;
        private final LoginToken okToken;
        private final Subject okSubject;

        DummyResolver() {
            okToken = makeLoginToken();
            okSubject = makeSubject();
            successfulResolves = 0;
            failedResolves = 0;
        }

        @Override
        public Subject resolve(LoginToken token) {
            if (token == okToken) {
                successfulResolves++;
                return okSubject;
            }
            failedResolves++;
            return null;
        }

        public LoginToken getOkToken() {
            return okToken;
        }

        public int getSuccessfulResolves() {
            return successfulResolves;
        }

        public int getFailedResolves() {
            return failedResolves;
        }

        private Subject makeSubject() {
            final Set<Principal> princs = new HashSet<Principal>();
            final Set<Object> pubCreds = new HashSet<Object>();
            final Set<Object> privCreds = new HashSet<Object>();
            return new Subject(true, princs, pubCreds, privCreds);
        }
    }
}
